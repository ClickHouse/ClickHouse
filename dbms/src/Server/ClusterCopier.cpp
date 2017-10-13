#include "ClusterCopier.h"
#include <boost/program_options.hpp>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/Application.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/File.h>
#include <chrono>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getFQDNOrHostName.h>
#include <Client/Connection.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/DescribeStreamFactory.h>

#include <common/logger_useful.h>
#include <common/ApplicationServerExt.h>
#include <Parsers/ASTCheckQuery.h>
#include <Common/typeid_cast.h>
#include <Common/ClickHouseRevision.h>
#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Databases/DatabaseMemory.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/Operators.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Common/isLocalAddress.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTDropQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <DataStreams/copyData.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <common/ThreadPool.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <IO/ReadBufferFromString.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/executeQuery.h>
#include <IO/WriteBufferNull.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
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


struct TaskCluster;
struct TaskTable;
struct TaskShard;
struct TaskPartition;


enum class TaskState
{
    Started = 0,
    Finished,
    Unknown
};


struct TaskStateWithOwner
{
    TaskStateWithOwner() = default;
    TaskStateWithOwner(TaskState state, const String & owner) : state(state), owner(owner) {}

    TaskState state{TaskState::Unknown};
    String owner;

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


struct TaskPartition
{
    TaskPartition(TaskShard & parent, const String & name_) : task_shard(parent), name(name_) {}

    String getCommonPartitionZooKeeperPath() const;
    String getZooKeeperPath() const;

    TaskShard & task_shard;
    String name;

    String create_query_pull;
    ASTPtr create_query_pull_ast;
    ASTPtr create_query_push;
};

using TasksPartition = std::vector<TaskPartition>;


using ShardInfo = Cluster::ShardInfo;

struct TaskShard
{
    TaskShard(TaskTable & parent, const ShardInfo & info_) : task_table(parent), info(info_) {}

    TaskTable & task_table;

    ShardInfo info;
    ConnectionPoolWithFailover::Entry connection_entry;

    TasksPartition partitions;
};

using TaskShardPtr = TaskShard *;
using TasksShard = std::vector<TaskShard>;
using TasksShardPtrs = std::vector<TaskShard *>;


struct TaskTable
{
    TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix,
                  const String & table_key);

    TaskCluster & task_cluster;

    /// Used as task ID
    String name_in_config;

    /// Source cluster and table
    String cluster_pull_name;
    DatabaseAndTableName table_pull;
    String db_table_pull;

    /// Destination cluster and table
    String cluster_push_name;
    DatabaseAndTableName table_push;
    String db_table_push;

    /// Storage of destination table
    String engine_push_str;
    ASTPtr engine_push_ast;

    /// Local Distributed table used to split data
    DatabaseAndTableName table_split;
    String sharding_key_str;
    ASTPtr sharding_key_ast;
    String engine_proxy_str;
    ASTPtr engine_proxy_ast;

    /// Additional WHERE expression to filter input data
    String where_condition_str;
    ASTPtr where_condition_ast;

    /// Resolved clusters
    ClusterPtr cluster_pull;
    ClusterPtr cluster_push;

    struct Shards
    {
        TasksShard all_shards;
        TasksShardPtrs all_shards_prioritized;

        TasksShardPtrs local_shards;
        TasksShardPtrs remote_shards;
    };

    /// Prioritized list of shards
    Shards shards_pull;

    template <class URNG>
    void initShards(URNG && urng);
};

using TasksTable = std::list<TaskTable>;


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


String TaskPartition::getCommonPartitionZooKeeperPath() const
{
    return task_shard.task_table.task_cluster.task_zookeeper_path                   // root
           + "/table_" + escapeForFileName(task_shard.task_table.name_in_config)    // table_test.hits
           + "/" + name;                                                            // 201701
}

String TaskPartition::getZooKeeperPath() const
{
    return getCommonPartitionZooKeeperPath()            // /root/table_test.hits/201701
        + "/" + toString(task_shard.info.shard_num);    // 1 (the first shard)
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
    db_table_pull = getDatabaseDotTable(table_pull);

    table_push.first = config.getString(table_prefix + "database_push");
    table_push.second = config.getString(table_prefix + "table_push");
    db_table_push = getDatabaseDotTable(table_push);

    engine_push_str = config.getString(table_prefix + "engine");
    {
        ParserStorage parser_storage;
        engine_push_ast = parseQuery(parser_storage, engine_push_str);
    }

    sharding_key_str = config.getString(table_prefix + "sharding_key");
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        sharding_key_ast = parseQuery(parser_expression, sharding_key_str);
        engine_proxy_ast = createASTStorageDistributed(cluster_pull_name, table_pull.first, table_pull.second, sharding_key_ast);
        engine_proxy_str = queryToString(engine_proxy_ast);

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
}


template<class URNG>
void TaskTable::initShards(URNG && urng)
{
    for (auto & shard_info : cluster_pull->getShardsInfo())
        shards_pull.all_shards.emplace_back(*this, shard_info);

    auto has_local_addresses = [] (const ShardInfo & info, const ClusterPtr & cluster) -> bool
    {
        if (info.isLocal()) /// isLocal() checks ports that aren't match
            return true;

        for (auto & address : cluster->getShardsAddresses().at(info.shard_num - 1))
        {
            if (isLocalAddress(address.resolved_address))
                return true;
        }

        return false;
    };

    for (TaskShard & shard : shards_pull.all_shards)
    {
        if (has_local_addresses(shard.info, cluster_pull))
            shards_pull.local_shards.push_back(&shard);
        else
            shards_pull.remote_shards.push_back(&shard);
    }

    // maybe it is not necessary to shuffle local addresses
    std::shuffle(shards_pull.local_shards.begin(), shards_pull.local_shards.end(), std::forward<URNG>(urng));
    std::shuffle(shards_pull.remote_shards.begin(), shards_pull.remote_shards.end(), std::forward<URNG>(urng));

    shards_pull.all_shards_prioritized.insert(shards_pull.all_shards_prioritized.end(),
                                              shards_pull.local_shards.begin(), shards_pull.local_shards.end());
    shards_pull.all_shards_prioritized.insert(shards_pull.all_shards_prioritized.end(),
                                              shards_pull.remote_shards.begin(), shards_pull.remote_shards.end());
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
            auto & shards = task_table.shards_pull.all_shards_prioritized;
            LOG_DEBUG(log, "There are " << shards.size() << " tasks for table " << task_table.name_in_config);

            for (TaskShardPtr task_shard : shards)
            {
                if (task_shard->info.pool == nullptr)
                {
                    throw Exception("It is impossible to have only local shards, at least port number must be different",
                                    ErrorCodes::LOGICAL_ERROR);
                }

                LOG_DEBUG(log, "Set up table task " << task_table.name_in_config << " ("
                               << "cluster " << task_table.cluster_pull_name
                               << ", table " << task_table.db_table_pull
                               << ", shard " << task_shard->info.shard_num << ")");

                LOG_DEBUG(log, "There are "
                    << task_table.shards_pull.local_shards.size() << " local shards, and "
                    << task_table.shards_pull.remote_shards.size() << " remote ones");

                task_shard->connection_entry = task_shard->info.pool->get(&task_cluster->settings_pull);
                LOG_DEBUG(log, "Will use " << task_shard->connection_entry->getDescription());

                Strings partitions = getRemotePartitions(task_table.table_pull, *task_shard->connection_entry, &task_cluster->settings_pull);
                for (const String & partition_name : partitions)
                    task_shard->partitions.emplace_back(*task_shard, partition_name);

                LOG_DEBUG(log, "Will fetch " << task_shard->partitions.size() << " parts");
            }
        }

        auto zookeeper = getZooKeeper();
        zookeeper->createAncestors(getWorkersPath() + "/");
    }

    void process()
    {
        for (TaskTable & task_table : task_cluster->table_tasks)
        {
            for (TaskShardPtr task_shard : task_table.shards_pull.all_shards_prioritized)
            {
                for (TaskPartition & task_partition : task_shard->partitions)
                {
                    processPartitionTask(task_partition);
                }
            }
        }
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
            auto zookeeper = getZooKeeper();

            zkutil::Stat stat;
            zookeeper->get(getWorkersPath(), &stat);

            if (stat.numChildren >= task_cluster->max_workers)
            {
                LOG_DEBUG(log, "Too many workers (" << stat.numChildren << ", maximum " << task_cluster->max_workers << ")"
                    << ". Postpone processing " << task_description);

                using namespace std::literals::chrono_literals;
                std::this_thread::sleep_for(1s);
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

    bool processPartitionTask(TaskPartition & task_partition)
    {
        auto zookeeper = getZooKeeper();

        String partition_task_node = task_partition.getZooKeeperPath();
        String partition_task_active_node = partition_task_node + "/active_worker";
        String partition_task_status_node = partition_task_node + "/state";

        /// Load balancing
        auto worker_node_holder = createWorkerNodeAndWaitIfNeed(zookeeper, partition_task_node);

        /// Create ephemeral node to mark that we are active
        zookeeper->createAncestors(partition_task_active_node);
        zkutil::EphemeralNodeHolderPtr partition_task_node_holder;
        try
        {
            partition_task_node_holder = zkutil::EphemeralNodeHolder::create(partition_task_active_node, *zookeeper, host_id);
        }
        catch (const zkutil::KeeperException & e)
        {
            if (e.code == ZNODEEXISTS)
            {
                LOG_DEBUG(log, "Someone is already processing " << partition_task_node);
                return false;
            }

            throw;
        }

        TaskShard & task_shard = task_partition.task_shard;
        TaskTable & task_table = task_shard.task_table;

        /// We need to update table definitions for each part, it could be changed after ALTER

        String create_query_pull_str = getRemoteCreateTable(task_table.table_pull, *task_shard.connection_entry,
                                                            &task_cluster->settings_pull);

        ParserCreateQuery parser_create_query;
        ASTPtr create_query_pull_ast = parseQuery(parser_create_query, create_query_pull_str);

        DatabaseAndTableName table_pull(working_database_name, ".pull." + task_table.name_in_config);
        DatabaseAndTableName table_split(working_database_name, ".split." + task_table.name_in_config);

        String table_pull_cluster_name = ".pull." + task_table.cluster_pull_name;
        size_t current_shard_index = task_shard.info.shard_num - 1;
        ClusterPtr cluster_pull_current_shard = task_table.cluster_pull->getClusterWithSingleShard(current_shard_index);
        context.setCluster(table_pull_cluster_name, cluster_pull_current_shard);

        auto storage_pull_ast = createASTStorageDistributed(table_pull_cluster_name, task_table.table_pull.first, task_table.table_pull.second);
        const auto & storage_split_ast = task_table.engine_proxy_ast;

        auto create_table_pull_ast = rewriteCreateQueryStorage(create_query_pull_ast, table_pull, storage_pull_ast);
        auto create_table_split_ast = rewriteCreateQueryStorage(create_query_pull_ast, table_split, storage_split_ast);

        LOG_DEBUG(log, "Create current pull table. Query: " << queryToString(create_table_pull_ast));
        dropAndCreateLocalTable(create_table_pull_ast);

        LOG_DEBUG(log, "Create split table. Query: " << queryToString(create_table_split_ast));
        dropAndCreateLocalTable(create_table_split_ast);

        auto create_query_push_ast = rewriteCreateQueryStorage(create_query_pull_ast, task_table.table_push, task_table.engine_push_ast);
        LOG_DEBUG(log, "Push table create query: " << queryToString(create_query_push_ast));

        TaskStateWithOwner start_state(TaskState::Started, host_id);
        auto code = zookeeper->tryCreate(partition_task_status_node, start_state.toString(), zkutil::CreateMode::Persistent);

        if (code == ZNODEEXISTS)
        {
            auto status = TaskStateWithOwner::fromString(zookeeper->get(partition_task_status_node));

            if (status.state == TaskState::Finished)
            {
                LOG_DEBUG(log, "Task " << partition_task_node << " has been executed by " << status.owner);
                return true;
            }
            else
            {
                LOG_DEBUG(log, "Found abandoned task " << partition_task_node);
                /// Restart shard
                return false;
            }
        }
        else if (code != ZOK)
            throw zkutil::KeeperException(code, partition_task_status_node);

        /// Try create table (if not exists) on each shard
        {
            auto query_ast = create_query_push_ast->clone();
            typeid_cast<ASTCreateQuery &>(*query_ast).if_not_exists = true;
            String query = queryToString(query_ast);

            LOG_DEBUG(log, "Create remote tables " << query);
            executeQueryOnOneReplicaAtLeast(task_table.cluster_push, query_ast, query, &task_cluster->settings_push);
        }

        /// Do the main action
        {
            String query;
            {
                std::stringstream ss;
                ss << "INSERT INTO " << getDatabaseDotTable(table_split)
                   << " SELECT * FROM " << getDatabaseDotTable(table_pull)
                   << " WHERE (_part LIKE '" << task_partition.name << "%')";

                if (!task_table.where_condition_str.empty())
                    ss << " AND (" + task_table.where_condition_str + ")";

                query = ss.str();
            }

            LOG_DEBUG(log, "Executing query: " << query);

            try
            {
                Context local_context = context;
                local_context.getSettingsRef() = task_cluster->settings_push;

                ReadBufferFromString istr(query);
                WriteBufferNull ostr;
                executeQuery(istr, ostr, false, local_context, {});
            }
            catch (...)
            {
                tryLogCurrentException(log);
            }
        }

        TaskStateWithOwner state_finish(TaskState::Started, host_id);
        zkutil::Stat stat;
        code = zookeeper->trySet(partition_task_status_node, state_finish.toString(), 0, &stat);

        if (code == ZBADVERSION)
        {
            LOG_DEBUG(log, "Someone made the node abandoned. Will refill partition");
            return false;
        }
        else if (code != ZOK)
            throw zkutil::KeeperException(code, partition_task_status_node);

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

    size_t executeQueryOnOneReplicaAtLeast(const ClusterPtr & cluster, const ASTPtr & query_ast, const String & query,
                                         const Settings * settings = nullptr) const
    {
        auto num_shards = cluster->getShardsInfo().size();
        std::vector<size_t> per_shard_num_sucessful_replicas(num_shards, 0);

        /// We need to execute query on one replica at least
        auto do_for_shard = [&] (size_t shard_index)
        {
            const Cluster::ShardInfo & shard = cluster->getShardsInfo().at(shard_index);
            size_t num_sucessful_replicas = 0;

            /// In that case we don't have local replicas, but do it just in case
            for (size_t i = 0; i < shard.getLocalNodeCount(); ++i)
            {
                InterpreterCreateQuery interpreter(query_ast, context);
                interpreter.execute();
                ++num_sucessful_replicas;
            }

            /// Will try to make as many as possible queries
            if (shard.hasRemoteConnections())
            {
                std::vector<IConnectionPool::Entry> connections = shard.pool->getMany(settings, PoolMode::GET_ALL);

                for (auto & connection : connections)
                {
                    if (!connection.isNull())
                    {
                        RemoteBlockInputStream stream(*connection, query, context, settings);
                        NullBlockOutputStream output;
                        try
                        {
                            copyData(stream, output);
                            ++num_sucessful_replicas;
                        }
                        catch (const Exception & e)
                        {
                            tryLogCurrentException(log);
                        }
                    }
                }
            }

            per_shard_num_sucessful_replicas[shard_index] = num_sucessful_replicas;
        };

        ThreadPool thread_pool(getNumberOfPhysicalCPUCores());

        for (size_t shard_index = 0; shard_index < num_shards; ++shard_index)
            thread_pool.schedule([=] { do_for_shard(shard_index); });
        thread_pool.wait();

        size_t sucessful_shards = 0;
        for (size_t num_replicas : per_shard_num_sucessful_replicas)
            sucessful_shards += (num_replicas > 0);

        return sucessful_shards;
    }

    String getTableStructureAndCheckConsistency(TaskTable & table_task)
    {
        InterpreterCheckQuery::RemoteTablesInfo remotes_info;
        remotes_info.cluster = table_task.cluster_pull;
        remotes_info.remote_database = table_task.table_pull.first;
        remotes_info.remote_table = table_task.table_pull.second;

        Context local_context = context;
        InterpreterCheckQuery check_table(std::move(remotes_info), local_context);

        BlockIO io = check_table.execute();
        if (io.out != nullptr)
            throw Exception("Expected empty io.out", ErrorCodes::LOGICAL_ERROR);

        String columns_structure;
        size_t rows = 0;

        Block block;
        while ((block = io.in->read()))
        {
            auto & structure_col = typeid_cast<ColumnString &>(*block.getByName("structure").column);
            auto & structure_class_col = typeid_cast<ColumnUInt32 &>(*block.getByName("structure_class").column);

            for (size_t i = 0; i < block.rows(); ++i)
            {
                if (structure_class_col.getElement(i) != 0)
                    throw Exception("Structures of table " + table_task.db_table_pull + " are different on cluster " +
                                        table_task.cluster_pull_name, ErrorCodes::BAD_ARGUMENTS);

                if (rows == 0)
                    columns_structure = structure_col.getDataAt(i).toString();

                ++rows;
            }
        }

        return columns_structure;
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

    ConfigurationPtr task_cluster_config;
    std::unique_ptr<TaskCluster> task_cluster;

    zkutil::ZooKeeperPtr current_zookeeper;

    Context & context;
    Poco::Logger * log;
};


class ClusterCopierApp : public Poco::Util::Application
{
public:

    void initialize(Poco::Util::Application & self) override
    {
        Poco::Util::Application::initialize(self);
    }

    void init(int argc, char ** argv)
    {
        /// Poco's program options are quite buggy, use boost's one
        namespace po = boost::program_options;

        po::options_description options_desc("Allowed options");
        options_desc.add_options()
            ("help", "produce this help message")
            ("config-file,c", po::value<std::string>(&config_xml)->required(), "path to config file with ZooKeeper config")
            ("task-path,p", po::value<std::string>(&task_path)->required(), "path to task in ZooKeeper")
            ("log-level", po::value<std::string>(&log_level)->default_value("debug"), "log level");

        po::positional_options_description positional_desc;
        positional_desc.add("config-file", 1);
        positional_desc.add("task-path", 1);

        po::variables_map options;
        po::store(po::command_line_parser(argc, argv).options(options_desc).positional(positional_desc).run(), options);
        po::notify(options);

        if (options.count("help"))
        {
            std::cerr << "Copies tables from one cluster to another" << std::endl;
            std::cerr << "Usage: clickhouse copier <config-file> <task-path>" << std::endl;
            std::cerr << options_desc << std::endl;
        }

        if (config_xml.empty() || !Poco::File(config_xml).exists())
            throw Exception("ZooKeeper configuration file " + config_xml + " doesn't exist", ErrorCodes::BAD_ARGUMENTS);

        setupLogging(log_level);
    }

    int main(const std::vector<std::string> & args) override
    {
        try
        {
            mainImpl();
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
            auto code = getCurrentExceptionCode();

            return (code ? code : -1);
        }

        return 0;
    }

    void mainImpl()
    {
        ConfigurationPtr zookeeper_configuration(new Poco::Util::XMLConfiguration(config_xml));
        auto log = &logger();

        /// Hostname + random id (to be able to run multiple copiers on the same host)
        process_id = Poco::UUIDGenerator().create().toString();
        host_id = escapeForFileName(getFQDNOrHostName()) + '#' + process_id;
        String clickhouse_path = Poco::Path::current() + "/" + process_id;

        LOG_INFO(log, "Starting clickhouse-copier ("
            << "id " << process_id << ", "
            << "host_id " << host_id << ", "
            << "path " << clickhouse_path << ", "
            << "revision " << ClickHouseRevision::get() << ")");

        auto context = std::make_unique<Context>(Context::createGlobal());
        SCOPE_EXIT(context->shutdown());

        context->setGlobalContext(*context);
        context->setApplicationType(Context::ApplicationType::LOCAL);
        context->setPath(clickhouse_path);

        registerFunctions();
        registerAggregateFunctions();
        registerTableFunctions();

        static const std::string default_database = "_local";
        context->addDatabase(default_database, std::make_shared<DatabaseMemory>(default_database));
        context->setCurrentDatabase(default_database);

        std::unique_ptr<ClusterCopier> copier(new ClusterCopier(
            zookeeper_configuration, task_path, host_id, default_database, *context));

        copier->init();
        copier->process();
    }


private:

    static void setupLogging(const std::string & log_level = "debug")
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel);
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
        formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel(log_level);
    }

    std::string config_xml;
    std::string task_path;
    std::string log_level = "error";

    std::string process_id;
    std::string host_id;
};

}


int mainEntryClickHouseClusterCopier(int argc, char ** argv)
{
    try
    {
        DB::ClusterCopierApp app;
        app.init(argc, argv);
        return app.run();
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();

        return (code ? code : -1);
    }
}
