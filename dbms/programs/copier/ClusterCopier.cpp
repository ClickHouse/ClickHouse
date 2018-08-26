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

#include <common/logger_useful.h>
#include <common/ThreadPool.h>
#include <daemon/OwnPatternFormatter.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/isLocalAddress.h>
#include <Common/typeid_cast.h>
#include <Common/ClickHouseRevision.h>
#include <Common/formatReadable.h>
#include <Common/DNSResolver.h>
#include <Common/CurrentThread.h>
#include <Common/escapeForFileName.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Client/Connection.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Formats/FormatSettings.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Storages/registerStorages.h>
#include <Storages/StorageDistributed.h>
#include <Databases/DatabaseMemory.h>
#include <Common/StatusFile.h>
#include <daemon/OwnPatternFormatter.h>


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

using TasksPartition = std::map<String, ShardPartition, std::greater<>>;
using ShardInfo = Cluster::ShardInfo;
using TaskShardPtr = std::shared_ptr<TaskShard>;
using TasksShard = std::vector<TaskShardPtr>;
using TasksTable = std::list<TaskTable>;
using ClusterPartitions = std::map<String, ClusterPartition, std::greater<>>;


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


/// Contains info about all shards that contain a partition
struct ClusterPartition
{
    double elapsed_time_seconds = 0;
    UInt64 bytes_copied = 0;
    UInt64 rows_copied = 0;
    UInt64 blocks_copied = 0;

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


struct MultiTransactionInfo
{
    int32_t code;
    Coordination::Requests requests;
    Coordination::Responses responses;
};


/// Atomically checks that is_dirty node is not exists, and made the remaining op
/// Returns relative number of failed operation in the second field (the passed op has 0 index)
static MultiTransactionInfo checkNoNodeAndCommit(
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & checking_node_path,
    Coordination::RequestPtr && op)
{
    MultiTransactionInfo info;
    info.requests.emplace_back(zkutil::makeCreateRequest(checking_node_path, "", zkutil::CreateMode::Persistent));
    info.requests.emplace_back(zkutil::makeRemoveRequest(checking_node_path, -1));
    info.requests.emplace_back(std::move(op));
    info.code = zookeeper->tryMulti(info.requests, info.responses);
    return info;
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
    std::stringstream ss;
    ss << "N" << numberInCluster()
       << " (having a replica " << getHostNameExample()
       << ", pull table " + getDatabaseDotTable(task_table.table_pull)
       << " of cluster " + task_table.cluster_pull_name << ")";
    return ss.str();
}

String DB::TaskShard::getHostNameExample() const
{
    auto & replicas = task_table.cluster_pull->getShardsAddresses().at(indexInCluster());
    return replicas.at(0).readableString();
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


static ShardPriority getReplicasPriority(const Cluster::Addresses & replicas, const std::string & local_hostname, UInt8 random)
{
    ShardPriority res;

    if (replicas.empty())
        return res;

    res.is_remote = 1;
    for (auto & replica : replicas)
    {
        if (isLocalAddress(DNSResolver::instance().resolveHost(replica.host_name)))
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

    auto set_default_value = [] (auto && setting, auto && default_value)
    {
        setting = setting.changed ? setting.value : default_value;
    };

    /// Override important settings
    settings_pull.readonly = 1;
    settings_push.insert_distributed_sync = 1;
    set_default_value(settings_pull.load_balancing, LoadBalancing::NEAREST_HOSTNAME);
    set_default_value(settings_pull.max_threads, 1);
    set_default_value(settings_pull.max_block_size, 8192UL);
    set_default_value(settings_pull.preferred_block_size_bytes, 0);
    set_default_value(settings_push.insert_distributed_timeout, 0);
}


} // end of an anonymous namespace


class ClusterCopier
{
public:

    ClusterCopier(const String & task_path_,
                  const String & host_id_,
                  const String & proxy_database_name_,
                  Context & context_)
    :
        task_zookeeper_path(task_path_),
        host_id(host_id_),
        working_database_name(proxy_database_name_),
        context(context_),
        log(&Poco::Logger::get("ClusterCopier"))
    {
    }

    void init()
    {
        auto zookeeper = context.getZooKeeper();

        task_description_watch_callback = [this] (const Coordination::WatchResponse &)
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

        LOG_DEBUG(log, "Will process " << task_cluster->table_tasks.size() << " table tasks");

        /// Do not initialize tables, will make deferred initialization in process()

        zookeeper->createAncestors(getWorkersPathVersion() + "/");
        zookeeper->createAncestors(getWorkersPath() + "/");
    }

    template <typename T>
    decltype(auto) retry(T && func, size_t max_tries = 100)
    {
        std::exception_ptr exception;

        for (size_t try_number = 1; try_number <= max_tries; ++try_number)
        {
            try
            {
                return func();
            }
            catch (...)
            {
                exception = std::current_exception();
                if (try_number < max_tries)
                {
                    tryLogCurrentException(log, "Will retry");
                    std::this_thread::sleep_for(default_sleep_time);
                }
            }
        }

        std::rethrow_exception(exception);
    }


    void discoverShardPartitions(const TaskShardPtr & task_shard)
    {
        TaskTable & task_table = task_shard->task_table;

        LOG_INFO(log, "Discover partitions of shard " << task_shard->getDescription());

        auto get_partitions = [&] () { return getShardPartitions(*task_shard); };
        auto existing_partitions_names = retry(get_partitions, 60);
        Strings filtered_partitions_names;
        Strings missing_partitions;

        /// Check that user specified correct partition names
        auto check_partition_format = [] (const DataTypePtr & type, const String & partition_text_quoted)
        {
            MutableColumnPtr column_dummy = type->createColumn();
            ReadBufferFromString rb(partition_text_quoted);

            try
            {
                type->deserializeTextQuoted(*column_dummy, rb, FormatSettings());
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
                /// Check that user specified correct partition names
                check_partition_format(task_shard->partition_key_column.type, partition_name);

                auto it = existing_partitions_names.find(partition_name);

                /// Do not process partition if it is not in enabled_partitions list
                if (it == existing_partitions_names.end())
                {
                    missing_partitions.emplace_back(partition_name);
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
            task_shard->checked_partitions.emplace(partition_name, true);
        }

        if (!missing_partitions.empty())
        {
            std::stringstream ss;
            for (const String & missing_partition : missing_partitions)
                ss << " " << missing_partition;

            LOG_WARNING(log, "There are no " << missing_partitions.size() << " partitions from enabled_partitions in shard "
                             << task_shard->getDescription() << " :" << ss.str());
        }

        LOG_DEBUG(log, "Will copy " << task_shard->partition_tasks.size() << " partitions from shard " << task_shard->getDescription());
    }

    /// Compute set of partitions, assume set of partitions aren't changed during the processing
    void discoverTablePartitions(TaskTable & task_table, size_t num_threads = 0)
    {
        /// Fetch partitions list from a shard
        {
            ThreadPool thread_pool(num_threads ? num_threads : 2 * getNumberOfPhysicalCPUCores());

            for (const TaskShardPtr & task_shard : task_table.all_shards)
                thread_pool.schedule([this, task_shard]() { discoverShardPartitions(task_shard); });

            LOG_DEBUG(log, "Waiting for " << thread_pool.active() << " setup jobs");
            thread_pool.wait();
        }
    }

    void reloadTaskDescription()
    {
        auto zookeeper = context.getZooKeeper();
        task_description_watch_zookeeper = zookeeper;

        String task_config_str;
        Coordination::Stat stat;
        int code;

        zookeeper->tryGetWatch(task_description_path, task_config_str, &stat, task_description_watch_callback, &code);
        if (code)
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
        bool is_outdated_version = task_descprtion_current_version != version_to_update;
        bool is_expired_session = !task_description_watch_zookeeper || task_description_watch_zookeeper->expired();

        if (!is_outdated_version && !is_expired_session)
            return;

        LOG_DEBUG(log, "Updating task description");
        reloadTaskDescription();

        task_descprtion_current_version = version_to_update;
    }

    void process()
    {
        for (TaskTable & task_table : task_cluster->table_tasks)
        {
            LOG_INFO(log, "Process table task " << task_table.table_id << " with "
                          << task_table.all_shards.size() << " shards, " << task_table.local_shards.size() << " of them are local ones");

            if (task_table.all_shards.empty())
                continue;

            /// Discover partitions of each shard and total set of partitions
            if (!task_table.has_enabled_partitions)
            {
                /// If there are no specified enabled_partitions, we must discover them manually
                discoverTablePartitions(task_table);

                /// After partitions of each shard are initialized, initialize cluster partitions
                for (const TaskShardPtr & task_shard : task_table.all_shards)
                {
                    for (const auto & partition_elem : task_shard->partition_tasks)
                    {
                        const String & partition_name = partition_elem.first;
                        task_table.cluster_partitions.emplace(partition_name, ClusterPartition{});
                    }
                }

                for (auto & partition_elem : task_table.cluster_partitions)
                {
                    const String & partition_name = partition_elem.first;

                    for (const TaskShardPtr & task_shard : task_table.all_shards)
                        task_shard->checked_partitions.emplace(partition_name);

                    task_table.ordered_partition_names.emplace_back(partition_name);
                }
            }
            else
            {
                /// If enabled_partitions are specified, assume that each shard has all partitions
                /// We will refine partition set of each shard in future

                for (const String & partition_name : task_table.enabled_partitions)
                {
                    task_table.cluster_partitions.emplace(partition_name, ClusterPartition{});
                    task_table.ordered_partition_names.emplace_back(partition_name);
                }
            }

            task_table.watch.restart();

            /// Retry table processing
            bool table_is_done = false;
            for (size_t num_table_tries = 0; num_table_tries < max_table_tries; ++num_table_tries)
            {
                if (tryProcessTable(task_table))
                {
                    table_is_done = true;
                    break;
                }
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


protected:

    String getWorkersPath() const
    {
        return task_cluster->task_zookeeper_path + "/task_active_workers";
    }

    String getWorkersPathVersion() const
    {
        return getWorkersPath() + "_version";
    }

    String getCurrentWorkerNodePath() const
    {
        return getWorkersPath() + "/" + host_id;
    }

    zkutil::EphemeralNodeHolder::Ptr createTaskWorkerNodeAndWaitIfNeed(const zkutil::ZooKeeperPtr & zookeeper,
                                                                       const String & description, bool unprioritized)
    {
        std::chrono::milliseconds current_sleep_time = default_sleep_time;
        static constexpr std::chrono::milliseconds max_sleep_time(30000); // 30 sec

        if (unprioritized)
            std::this_thread::sleep_for(current_sleep_time);

        String workers_version_path = getWorkersPathVersion();
        String workers_path = getWorkersPath();
        String current_worker_path = getCurrentWorkerNodePath();

        size_t num_bad_version_errors = 0;

        while (true)
        {
            updateConfigIfNeeded();

            Coordination::Stat stat;
            zookeeper->get(workers_version_path, &stat);
            auto version = stat.version;
            zookeeper->get(workers_path, &stat);

            if (static_cast<size_t>(stat.numChildren) >= task_cluster->max_workers)
            {
                LOG_DEBUG(log, "Too many workers (" << stat.numChildren << ", maximum " << task_cluster->max_workers << ")"
                    << ". Postpone processing " << description);

                if (unprioritized)
                    current_sleep_time = std::min(max_sleep_time, current_sleep_time + default_sleep_time);

                std::this_thread::sleep_for(current_sleep_time);
                num_bad_version_errors = 0;
            }
            else
            {
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeSetRequest(workers_version_path, description, version));
                ops.emplace_back(zkutil::makeCreateRequest(current_worker_path, description, zkutil::CreateMode::Ephemeral));
                Coordination::Responses responses;
                auto code = zookeeper->tryMulti(ops, responses);

                if (code == Coordination::ZOK || code == Coordination::ZNODEEXISTS)
                    return std::make_shared<zkutil::EphemeralNodeHolder>(current_worker_path, *zookeeper, false, false, description);

                if (code == Coordination::ZBADVERSION)
                {
                    ++num_bad_version_errors;

                    /// Try to make fast retries
                    if (num_bad_version_errors > 3)
                    {
                        LOG_DEBUG(log, "A concurrent worker has just been added, will check free worker slots again");
                        std::chrono::milliseconds random_sleep_time(std::uniform_int_distribution<int>(1, 1000)(task_cluster->random_engine));
                        std::this_thread::sleep_for(random_sleep_time);
                        num_bad_version_errors = 0;
                    }
                }
                else
                    throw Coordination::Exception(code);
            }
        }
    }

    /** Checks that the whole partition of a table was copied. We should do it carefully due to dirty lock.
     * State of some task could be changed during the processing.
     * We have to ensure that all shards have the finished state and there are no dirty flag.
     * Moreover, we have to check status twice and check zxid, because state could be changed during the checking.
     */
    bool checkPartitionIsDone(const TaskTable & task_table, const String & partition_name, const TasksShard & shards_with_partition)
    {
        LOG_DEBUG(log, "Check that all shards processed partition " << partition_name << " successfully");

        auto zookeeper = context.getZooKeeper();

        Strings status_paths;
        for (auto & shard : shards_with_partition)
        {
            ShardPartition & task_shard_partition = shard->partition_tasks.find(partition_name)->second;
            status_paths.emplace_back(task_shard_partition.getShardStatusPath());
        }

        std::vector<int64_t> zxid1, zxid2;

        try
        {
            std::vector<zkutil::ZooKeeper::FutureGet> get_futures;
            for (const String & path : status_paths)
                get_futures.emplace_back(zookeeper->asyncGet(path));

            // Check that state is Finished and remember zxid
            for (auto & future : get_futures)
            {
                auto res = future.get();

                TaskStateWithOwner status = TaskStateWithOwner::fromString(res.data);
                if (status.state != TaskState::Finished)
                {
                    LOG_INFO(log, "The task " << res.data << " is being rewritten by " << status.owner << ". Partition will be rechecked");
                    return false;
                }

                zxid1.push_back(res.stat.pzxid);
            }

            // Check that partition is not dirty
            if (zookeeper->exists(task_table.getPartitionIsDirtyPath(partition_name)))
            {
                LOG_INFO(log, "Partition " << partition_name << " become dirty");
                return false;
            }

            get_futures.clear();
            for (const String & path : status_paths)
                get_futures.emplace_back(zookeeper->asyncGet(path));

            // Remember zxid of states again
            for (auto & future : get_futures)
            {
                auto res = future.get();
                zxid2.push_back(res.stat.pzxid);
            }
        }
        catch (const Coordination::Exception & e)
        {
            LOG_INFO(log, "A ZooKeeper error occurred while checking partition " << partition_name
                          << ". Will recheck the partition. Error: " << e.displayText());
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
                ColumnDefaultKind kind = columnDefaultKindFromString(column.default_specifier);
                if (kind == ColumnDefaultKind::Materialized || kind == ColumnDefaultKind::Alias)
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
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::ZNODEEXISTS)
            {
                LOG_DEBUG(log, "Partition " << task_partition.name << " is cleaning now by somebody, sleep");
                std::this_thread::sleep_for(default_sleep_time);
                return false;
            }

            throw;
        }

        Coordination::Stat stat;
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
        Coordination::Requests requests;
        requests.emplace_back(zkutil::makeRemoveRequest(dirt_cleaner_path, -1));
        requests.emplace_back(zkutil::makeRemoveRequest(is_dirty_flag_path, -1));
        zookeeper->multi(requests);

        LOG_INFO(log, "Partition " << task_partition.name << " was dropped on cluster " << task_table.cluster_push_name);
        return true;
    }


    static constexpr size_t max_table_tries = 1000;
    static constexpr size_t max_shard_partition_tries = 600;

    bool tryProcessTable(TaskTable & task_table)
    {
        /// An heuristic: if previous shard is already done, then check next one without sleeps due to max_workers constraint
        bool previous_shard_is_instantly_finished = false;

        /// Process each partition that is present in cluster
        for (const String & partition_name : task_table.ordered_partition_names)
        {
            if (!task_table.cluster_partitions.count(partition_name))
                throw Exception("There are no expected partition " + partition_name + ". It is a bug", ErrorCodes::LOGICAL_ERROR);

            ClusterPartition & cluster_partition = task_table.cluster_partitions[partition_name];

            Stopwatch watch;
            TasksShard expected_shards;
            size_t num_failed_shards = 0;

            ++cluster_partition.total_tries;

            LOG_DEBUG(log, "Processing partition " << partition_name << " for the whole cluster");

            /// Process each source shard having current partition and copy current partition
            /// NOTE: shards are sorted by "distance" to current host
            for (const TaskShardPtr & shard : task_table.all_shards)
            {
                /// Does shard have a node with current partition?
                if (shard->partition_tasks.count(partition_name) == 0)
                {
                    /// If not, did we check existence of that partition previously?
                    if (shard->checked_partitions.count(partition_name) == 0)
                    {
                        auto check_shard_has_partition = [&] () { return checkShardHasPartition(*shard, partition_name); };
                        bool has_partition = retry(check_shard_has_partition);

                        shard->checked_partitions.emplace(partition_name);

                        if (has_partition)
                        {
                            shard->partition_tasks.emplace(partition_name, ShardPartition(*shard, partition_name));
                            LOG_DEBUG(log, "Discovered partition " << partition_name << " in shard " << shard->getDescription());
                        }
                        else
                        {
                            LOG_DEBUG(log, "Found that shard " << shard->getDescription() << " does not contain current partition " << partition_name);
                            continue;
                        }
                    }
                    else
                    {
                        /// We have already checked that partition, but did not discover it
                        previous_shard_is_instantly_finished = true;
                        continue;
                    }
                }

                auto it_shard_partition = shard->partition_tasks.find(partition_name);
                if (it_shard_partition == shard->partition_tasks.end())
                     throw Exception("There are no such partition in a shard. This is a bug.", ErrorCodes::LOGICAL_ERROR);
                auto & partition = it_shard_partition->second;

                expected_shards.emplace_back(shard);

                /// Do not sleep if there is a sequence of already processed shards to increase startup
                bool is_unprioritized_task = !previous_shard_is_instantly_finished && shard->priority.is_remote;
                PartitionTaskStatus task_status = PartitionTaskStatus::Error;
                bool was_error = false;
                for (size_t try_num = 0; try_num < max_shard_partition_tries; ++try_num)
                {
                    task_status = tryProcessPartitionTask(partition, is_unprioritized_task);

                    /// Exit if success
                    if (task_status == PartitionTaskStatus::Finished)
                        break;

                    was_error = true;

                    /// Skip if the task is being processed by someone
                    if (task_status == PartitionTaskStatus::Active)
                        break;

                    /// Repeat on errors
                    std::this_thread::sleep_for(default_sleep_time);
                }

                if (task_status == PartitionTaskStatus::Error)
                    ++num_failed_shards;

                previous_shard_is_instantly_finished = !was_error;
            }

            cluster_partition.elapsed_time_seconds += watch.elapsedSeconds();

            /// Check that whole cluster partition is done
            /// Firstly check number failed partition tasks, than look into ZooKeeper and ensure that each partition is done
            bool partition_is_done = num_failed_shards == 0;
            try
            {
                partition_is_done = partition_is_done && checkPartitionIsDone(task_table, partition_name, expected_shards);
            }
            catch (...)
            {
                tryLogCurrentException(log);
                partition_is_done = false;
            }

            if (partition_is_done)
            {
                task_table.finished_cluster_partitions.emplace(partition_name);

                task_table.bytes_copied += cluster_partition.bytes_copied;
                task_table.rows_copied += cluster_partition.rows_copied;
                double elapsed = cluster_partition.elapsed_time_seconds;

                LOG_INFO(log, "It took " << std::fixed << std::setprecision(2) << elapsed << " seconds to copy partition " << partition_name
                         << ": " << formatReadableSizeWithDecimalSuffix(cluster_partition.bytes_copied) << " uncompressed bytes"
                         << ", " << formatReadableQuantity(cluster_partition.rows_copied) << " rows"
                         << " and " << cluster_partition.blocks_copied << " source blocks are copied");

                if (cluster_partition.rows_copied)
                {
                    LOG_INFO(log, "Average partition speed: "
                        << formatReadableSizeWithDecimalSuffix(cluster_partition.bytes_copied / elapsed) << " per second.");
                }

                if (task_table.rows_copied)
                {
                    LOG_INFO(log, "Average table " << task_table.table_id << " speed: "
                        << formatReadableSizeWithDecimalSuffix(task_table.bytes_copied / elapsed) << " per second.");
                }
            }
        }

        size_t required_partitions = task_table.cluster_partitions.size();
        size_t finished_partitions = task_table.finished_cluster_partitions.size();
        bool table_is_done = finished_partitions >= required_partitions;

        if (!table_is_done)
        {
            LOG_INFO(log, "Table " + task_table.table_id + " is not processed yet."
                << "Copied " << finished_partitions << " of " << required_partitions << ", will retry");
        }

        return table_is_done;
    }


    /// Execution status of a task
    enum class PartitionTaskStatus
    {
        Active,
        Finished,
        Error,
    };

    PartitionTaskStatus tryProcessPartitionTask(ShardPartition & task_partition, bool is_unprioritized_task)
    {
        PartitionTaskStatus res;

        try
        {
            res = processPartitionTaskImpl(task_partition, is_unprioritized_task);
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occurred while processing partition " + task_partition.name);
            res = PartitionTaskStatus::Error;
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

    PartitionTaskStatus processPartitionTaskImpl(ShardPartition & task_partition, bool is_unprioritized_task)
    {
        TaskShard & task_shard = task_partition.task_shard;
        TaskTable & task_table = task_shard.task_table;
        ClusterPartition & cluster_partition = task_table.getClusterPartition(task_partition.name);

        auto zookeeper = context.getZooKeeper();

        String is_dirty_flag_path = task_partition.getCommonPartitionIsDirtyPath();
        String current_task_is_active_path = task_partition.getActiveWorkerPath();
        String current_task_status_path = task_partition.getShardStatusPath();

        /// Auxiliary functions:

        /// Creates is_dirty node to initialize DROP PARTITION
        auto create_is_dirty_node = [&] ()
        {
            auto code = zookeeper->tryCreate(is_dirty_flag_path, current_task_status_path, zkutil::CreateMode::Persistent);
            if (code && code != Coordination::ZNODEEXISTS)
                throw Coordination::Exception(code, is_dirty_flag_path);
        };

        /// Returns SELECT query filtering current partition and applying user filter
        auto get_select_query = [&] (const DatabaseAndTableName & from_table, const String & fields, String limit = "")
        {
            String query;
            query += "SELECT " + fields + " FROM " + getDatabaseDotTable(from_table);
            /// TODO: Bad, it is better to rewrite with ASTLiteral(partition_key_field)
            query += " WHERE (" + queryToString(task_table.engine_push_partition_key_ast) + " = " + task_partition.name + ")";
            if (!task_table.where_condition_str.empty())
                query += " AND (" + task_table.where_condition_str + ")";
            if (!limit.empty())
                query += " LIMIT " + limit;

            ParserQuery p_query(query.data() + query.size());
            return parseQuery(p_query, query, 0);
        };

        /// Load balancing
        auto worker_node_holder = createTaskWorkerNodeAndWaitIfNeed(zookeeper, current_task_status_path, is_unprioritized_task);

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
                tryLogCurrentException(log, "An error occurred when clean partition");
            }

            return PartitionTaskStatus::Error;
        }

        /// Create ephemeral node to mark that we are active and process the partition
        zookeeper->createAncestors(current_task_is_active_path);
        zkutil::EphemeralNodeHolderPtr partition_task_node_holder;
        try
        {
            partition_task_node_holder = zkutil::EphemeralNodeHolder::create(current_task_is_active_path, *zookeeper, host_id);
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::ZNODEEXISTS)
            {
                LOG_DEBUG(log, "Someone is already processing " << current_task_is_active_path);
                return PartitionTaskStatus::Active;
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
                    return PartitionTaskStatus::Finished;
                }

                // Task is abandoned, initialize DROP PARTITION
                LOG_DEBUG(log, "Task " << current_task_status_path << " has not been successfully finished by " << status.owner);

                create_is_dirty_node();
                return PartitionTaskStatus::Error;
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

                Block block = getBlockWithAllStreamData(InterpreterFactory::get(query_select_ast, local_context)->execute().in);
                count = (block) ? block.safeGetByPosition(0).column->getUInt(0) : 0;
            }

            if (count != 0)
            {
                Coordination::Stat stat_shards;
                zookeeper->get(task_partition.getPartitionShardsPath(), &stat_shards);

                if (stat_shards.numChildren == 0)
                {
                    LOG_WARNING(log, "There are no any workers for partition " << task_partition.name
                                     << ", but destination table contains " << count << " rows"
                                     << ". Partition will be dropped and refilled.");

                    create_is_dirty_node();
                    return PartitionTaskStatus::Error;
                }
            }
        }

        /// Try start processing, create node about it
        {
            String start_state = TaskStateWithOwner::getData(TaskState::Started, host_id);
            auto op_create = zkutil::makeCreateRequest(current_task_status_path, start_state, zkutil::CreateMode::Persistent);
            MultiTransactionInfo info = checkNoNodeAndCommit(zookeeper, is_dirty_flag_path, std::move(op_create));

            if (info.code)
            {
                zkutil::KeeperMultiException exception(info.code, info.requests, info.responses);

                if (exception.getPathForFirstFailedOp() == is_dirty_flag_path)
                {
                    LOG_INFO(log, "Partition " << task_partition.name << " is dirty and will be dropped and refilled");
                    return PartitionTaskStatus::Error;
                }

                throw exception;
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
                double value = std::uniform_real_distribution<>(0, 1)(task_table.task_cluster.random_engine);
                inject_fault = value < copy_fault_probability;
            }

            // Select all fields
            ASTPtr query_select_ast = get_select_query(task_shard.table_read_shard, "*", inject_fault ? "1" : "");

            LOG_DEBUG(log, "Executing SELECT query and pull from " << task_shard.getDescription()
                           << " : " << queryToString(query_select_ast));

            ASTPtr query_insert_ast;
            {
                String query;
                query += "INSERT INTO " + getDatabaseDotTable(task_shard.table_split_shard) + " VALUES ";

                ParserQuery p_query(query.data() + query.size());
                query_insert_ast = parseQuery(p_query, query, 0);

                LOG_DEBUG(log, "Executing INSERT query: " << query);
            }

            try
            {
                /// Custom INSERT SELECT implementation
                Context context_select = context;
                context_select.getSettingsRef() = task_cluster->settings_pull;

                Context context_insert = context;
                context_insert.getSettingsRef() = task_cluster->settings_push;

                BlockInputStreamPtr input;
                BlockOutputStreamPtr output;
                {
                    BlockIO io_select = InterpreterFactory::get(query_select_ast, context_select)->execute();
                    BlockIO io_insert = InterpreterFactory::get(query_insert_ast, context_insert)->execute();

                    input = io_select.in;
                    output = io_insert.out;
                }

                std::future<Coordination::ExistsResponse> future_is_dirty_checker;

                Stopwatch watch(CLOCK_MONOTONIC_COARSE);
                constexpr size_t check_period_milliseconds = 500;

                /// Will asynchronously check that ZooKeeper connection and is_dirty flag appearing while copy data
                auto cancel_check = [&] ()
                {
                    if (zookeeper->expired())
                        throw Exception("ZooKeeper session is expired, cancel INSERT SELECT", ErrorCodes::UNFINISHED);

                    if (!future_is_dirty_checker.valid())
                        future_is_dirty_checker = zookeeper->asyncExists(is_dirty_flag_path);

                    /// check_period_milliseconds should less than average insert time of single block
                    /// Otherwise, the insertion will slow a little bit
                    if (watch.elapsedMilliseconds() >= check_period_milliseconds)
                    {
                        Coordination::ExistsResponse status = future_is_dirty_checker.get();

                        if (status.error != Coordination::ZNONODE)
                            throw Exception("Partition is dirty, cancel INSERT SELECT", ErrorCodes::UNFINISHED);
                    }

                    return false;
                };

                /// Update statistics
                /// It is quite rough: bytes_copied don't take into account DROP PARTITION.
                auto update_stats = [&cluster_partition] (const Block & block)
                {
                    cluster_partition.bytes_copied += block.bytes();
                    cluster_partition.rows_copied += block.rows();
                    cluster_partition.blocks_copied += 1;
                };

                /// Main work is here
                copyData(*input, *output, cancel_check, update_stats);

                // Just in case
                if (future_is_dirty_checker.valid())
                    future_is_dirty_checker.get();

                if (inject_fault)
                    throw Exception("Copy fault injection is activated", ErrorCodes::UNFINISHED);
            }
            catch (...)
            {
                tryLogCurrentException(log, "An error occurred during copying, partition will be marked as dirty");
                return PartitionTaskStatus::Error;
            }
        }

        /// Finalize the processing, change state of current partition task (and also check is_dirty flag)
        {
            String state_finished = TaskStateWithOwner::getData(TaskState::Finished, host_id);
            auto op_set = zkutil::makeSetRequest(current_task_status_path, state_finished, 0);
            MultiTransactionInfo info = checkNoNodeAndCommit(zookeeper, is_dirty_flag_path, std::move(op_set));

            if (info.code)
            {
                zkutil::KeeperMultiException exception(info.code, info.requests, info.responses);

                if (exception.getPathForFirstFailedOp() == is_dirty_flag_path)
                    LOG_INFO(log, "Partition " << task_partition.name << " became dirty and will be dropped and refilled");
                else
                    LOG_INFO(log, "Someone made the node abandoned. Will refill partition. " << zkutil::ZooKeeper::error2string(info.code));

                return PartitionTaskStatus::Error;
            }
        }

        LOG_INFO(log, "Partition " << task_partition.name << " copied");
        return PartitionTaskStatus::Finished;
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
        return parseQuery(parser_create_query, create_query_pull_str, 0);
    }

    void createShardInternalTables(TaskShard & task_shard, bool create_split = true)
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

        dropAndCreateLocalTable(create_table_pull_ast);

        if (create_split)
            dropAndCreateLocalTable(create_table_split_ast);
    }


    std::set<String> getShardPartitions(TaskShard & task_shard)
    {
        createShardInternalTables(task_shard, false);

        TaskTable & task_table = task_shard.task_table;

        String query;
        {
            WriteBufferFromOwnString wb;
            wb << "SELECT DISTINCT " << queryToString(task_table.engine_push_partition_key_ast) << " AS partition FROM"
               << " " << getDatabaseDotTable(task_shard.table_read_shard) << " ORDER BY partition DESC";
            query = wb.str();
        }

        ParserQuery parser_query(query.data() + query.size());
        ASTPtr query_ast = parseQuery(parser_query, query, 0);

        LOG_DEBUG(log, "Computing destination partition set, executing query: " << query);

        Context local_context = context;
        local_context.setSettings(task_cluster->settings_pull);
        Block block = getBlockWithAllStreamData(InterpreterFactory::get(query_ast, local_context)->execute().in);

        std::set<String> res;
        if (block)
        {
            ColumnWithTypeAndName & column = block.getByPosition(0);
            task_shard.partition_key_column = column;

            for (size_t i = 0; i < column.column->size(); ++i)
            {
                WriteBufferFromOwnString wb;
                column.type->serializeTextQuoted(*column.column, i, wb, FormatSettings());
                res.emplace(wb.str());
            }
        }

        LOG_DEBUG(log, "There are " << res.size() << " destination partitions in shard " << task_shard.getDescription());

        return res;
    }

    bool checkShardHasPartition(TaskShard & task_shard, const String & partition_quoted_name)
    {
        createShardInternalTables(task_shard, false);

        TaskTable & task_table = task_shard.task_table;

        String query;
        {
            WriteBufferFromOwnString wb;
            wb << "SELECT 1"
               << " FROM "<< getDatabaseDotTable(task_shard.table_read_shard)
               << " WHERE " << queryToString(task_table.engine_push_partition_key_ast) << " = " << partition_quoted_name
               << " LIMIT 1";
            query = wb.str();
        }

        LOG_DEBUG(log, "Checking shard " << task_shard.getDescription() << " for partition "
                       << partition_quoted_name << " existence, executing query: " << query);

        ParserQuery parser_query(query.data() + query.size());
        ASTPtr query_ast = parseQuery(parser_query, query, 0);

        Context local_context = context;
        local_context.setSettings(task_cluster->settings_pull);
        return InterpreterFactory::get(query_ast, local_context)->execute().in->read().rows() != 0;
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
            query_ast = parseQuery(p_query, query, 0);
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

private:
    String task_zookeeper_path;
    String task_description_path;
    String host_id;
    String working_database_name;

    /// Auto update config stuff
    UInt64 task_descprtion_current_version = 1;
    std::atomic<UInt64> task_descprtion_version{1};
    Coordination::WatchCallback task_description_watch_callback;
    /// ZooKeeper session used to set the callback
    zkutil::ZooKeeperPtr task_description_watch_zookeeper;

    ConfigurationPtr task_cluster_initial_config;
    ConfigurationPtr task_cluster_current_config;
    Coordination::Stat task_descprtion_current_stat;

    std::unique_ptr<TaskCluster> task_cluster;

    bool is_safe_mode = false;
    double copy_fault_probability = 0.0;

    Context & context;
    Poco::Logger * log;

    std::chrono::milliseconds default_sleep_time{1000};
};


/// ClusterCopierApp


void ClusterCopierApp::initialize(Poco::Util::Application & self)
{
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

    /// Override variables for BaseDaemon
    if (config().has("log-level"))
        config().setString("logger.level", config().getString("log-level"));

    if (config().has("base-dir") || !config().has("logger.log"))
        config().setString("logger.log", process_path + "/log.log");

    if (config().has("base-dir") || !config().has("logger.errorlog"))
        config().setString("logger.errorlog", process_path + "/log.err.log");

    Base::initialize(self);
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
    Base::defineOptions(options);

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


void ClusterCopierApp::mainImpl()
{
    StatusFile status_file(process_path + "/status");

    auto log = &logger();
    LOG_INFO(log, "Starting clickhouse-copier ("
        << "id " << process_id << ", "
        << "host_id " << host_id << ", "
        << "path " << process_path << ", "
        << "revision " << ClickHouseRevision::get() << ")");

    auto context = std::make_unique<Context>(Context::createGlobal());
    SCOPE_EXIT(context->shutdown());

    context->setConfig(loaded_config.configuration);
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

    /// Initialize query scope just in case.
    CurrentThread::QueryScope query_scope(*context);

    auto copier = std::make_unique<ClusterCopier>(task_path, host_id, default_database, *context);
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
