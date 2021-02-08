#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/queryToString.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Databases/DatabaseReplicatedWorker.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/Cluster.h>
#include <common/getFQDNOrHostName.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/formatAST.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int REPLICA_IS_ALREADY_EXIST;
    extern const int DATABASE_REPLICATION_FAILED;
    extern const int UNKNOWN_DATABASE;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}

static constexpr const char * DROPPED_MARK = "DROPPED";

zkutil::ZooKeeperPtr DatabaseReplicated::getZooKeeper() const
{
    return global_context.getZooKeeper();
}

static inline String getHostID(const Context & global_context, const UUID & db_uuid)
{
    return Cluster::Address::toString(getFQDNOrHostName(), global_context.getTCPPort()) + ':' + toString(db_uuid);
}


DatabaseReplicated::~DatabaseReplicated() = default;

DatabaseReplicated::DatabaseReplicated(
    const String & name_,
    const String & metadata_path_,
    UUID uuid,
    const String & zookeeper_path_,
    const String & shard_name_,
    const String & replica_name_,
    const Context & context_)
    : DatabaseAtomic(name_, metadata_path_, uuid, "DatabaseReplicated (" + name_ + ")", context_)
    , zookeeper_path(zookeeper_path_)
    , shard_name(shard_name_)
    , replica_name(replica_name_)
{
    if (zookeeper_path.empty() || shard_name.empty() || replica_name.empty())
        throw Exception("ZooKeeper path, shard and replica names must be non-empty", ErrorCodes::BAD_ARGUMENTS);
    if (shard_name.find('/') != std::string::npos || replica_name.find('/') != std::string::npos)
        throw Exception("Shard and replica names should not contain '/'", ErrorCodes::BAD_ARGUMENTS);
    if (shard_name.find('|') != std::string::npos || replica_name.find('|') != std::string::npos)
        throw Exception("Shard and replica names should not contain '|'", ErrorCodes::BAD_ARGUMENTS);

    if (zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);

    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    if (!context_.hasZooKeeper())
    {
        throw Exception("Can't create replicated database without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    }
    //FIXME it will fail on startup if zk is not available

    auto current_zookeeper = global_context.getZooKeeper();

    if (!current_zookeeper->exists(zookeeper_path))
    {
        /// Create new database, multiple nodes can execute it concurrently
        createDatabaseNodesInZooKeeper(current_zookeeper);
    }

    replica_path = zookeeper_path + "/replicas/" + getFullReplicaName();

    String replica_host_id;
    if (current_zookeeper->tryGet(replica_path, replica_host_id))
    {
        String host_id = getHostID(global_context, db_uuid);
        if (replica_host_id != host_id)
            throw Exception(ErrorCodes::REPLICA_IS_ALREADY_EXIST,
                            "Replica {} of shard {} of replicated database at {} already exists. Replica host ID: '{}', current host ID: '{}'",
                            replica_name, shard_name, zookeeper_path, replica_host_id, host_id);

        log_entry_to_execute = parse<UInt32>(current_zookeeper->get(replica_path + "/log_ptr"));
    }
    else
    {
        /// Throws if replica with the same name was created concurrently
        createReplicaNodesInZooKeeper(current_zookeeper);
    }
}

String DatabaseReplicated::getFullReplicaName() const
{
    return shard_name + '|' + replica_name;
}

std::pair<String, String> DatabaseReplicated::parseFullReplicaName(const String & name)
{
    String shard;
    String replica;
    auto pos = name.find('|');
    if (pos == std::string::npos || name.find('|', pos + 1) != std::string::npos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect replica identifier: {}", name);
    shard = name.substr(0, pos);
    replica = name.substr(pos + 1);
    return {shard, replica};
}

ClusterPtr DatabaseReplicated::getCluster() const
{
    /// TODO Maintain up-to-date Cluster and allow to use it in Distributed tables
    Strings hosts;
    Strings host_ids;

    auto zookeeper = global_context.getZooKeeper();
    constexpr int max_retries = 10;
    int iteration = 0;
    bool success = false;
    while (++iteration <= max_retries)
    {
        host_ids.resize(0);
        Coordination::Stat stat;
        hosts = zookeeper->getChildren(zookeeper_path + "/replicas", &stat);
        if (hosts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No hosts found");
        Int32 cver = stat.cversion;
        std::sort(hosts.begin(), hosts.end());

        std::vector<zkutil::ZooKeeper::FutureGet> futures;
        futures.reserve(hosts.size());
        host_ids.reserve(hosts.size());
        for (const auto & host : hosts)
            futures.emplace_back(zookeeper->asyncTryGet(zookeeper_path + "/replicas/" + host));

        success = true;
        for (auto & future : futures)
        {
            auto res = future.get();
            if (res.error != Coordination::Error::ZOK)
                success = false;
            host_ids.emplace_back(res.data);
        }

        zookeeper->get(zookeeper_path + "/replicas", &stat);
        if (success && cver == stat.version)
            break;
    }
    if (!success)
        throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Cannot get consistent cluster snapshot");

    assert(!hosts.empty());
    assert(hosts.size() == host_ids.size());
    String current_shard = parseFullReplicaName(hosts.front()).first;
    std::vector<Strings> shards;
    shards.emplace_back();
    for (size_t i = 0; i < hosts.size(); ++i)
    {
        const auto & id = host_ids[i];
        if (id == DROPPED_MARK)
            continue;
        auto [shard, replica] = parseFullReplicaName(hosts[i]);
        auto pos = id.find(':');
        String host = id.substr(0, pos);
        if (shard != current_shard)
        {
            current_shard = shard;
            if (!shards.back().empty())
                shards.emplace_back();
        }
        shards.back().emplace_back(unescapeForFileName(host));
    }

    /// TODO make it configurable
    String username = "default";
    String password;

    return std::make_shared<Cluster>(global_context.getSettingsRef(), shards, username, password, global_context.getTCPPort(), false);
}

bool DatabaseReplicated::createDatabaseNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    current_zookeeper->createAncestors(zookeeper_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/counter", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/counter/cnt-", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/counter/cnt-", -1));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/max_log_ptr", "1", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/logs_to_keep", "1000", zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto res = current_zookeeper->tryMulti(ops, responses);
    if (res == Coordination::Error::ZOK)
        return true;
    if (res == Coordination::Error::ZNODEEXISTS)
        return false;

    zkutil::KeeperMultiException::check(res, ops, responses);
    assert(false);
    __builtin_unreachable();
}

void DatabaseReplicated::createReplicaNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    /// When creating new replica, use latest snapshot version as initial value of log_pointer
    //log_entry_to_execute = 0;   //FIXME

    /// Write host name to replica_path, it will protect from multiple replicas with the same name
    auto host_id = getHostID(global_context, db_uuid);

    /// On replica creation add empty entry to log. Can be used to trigger some actions on other replicas (e.g. update cluster info).
    DDLLogEntry entry;
    entry.hosts = {};
    entry.query = {};
    entry.initiator = {};

    String query_path_prefix = zookeeper_path + "/log/query-";
    String counter_prefix = zookeeper_path + "/counter/cnt-";
    String counter_path = current_zookeeper->create(counter_prefix, "", zkutil::CreateMode::EphemeralSequential);
    String query_path = query_path_prefix + counter_path.substr(counter_prefix.size());

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(replica_path, host_id, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_ptr", "0", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(query_path, entry.toString(), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeRemoveRequest(counter_path, -1));
    current_zookeeper->multi(ops);
}

void DatabaseReplicated::loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach)
{
    DatabaseAtomic::loadStoredObjects(context, has_force_restore_data_flag, force_attach);

    ddl_worker = std::make_unique<DatabaseReplicatedDDLWorker>(this, global_context);
    ddl_worker->startup();
}

void DatabaseReplicated::onUnexpectedLogEntry(const String & entry_name, const ZooKeeperPtr & zookeeper)
{
    /// We cannot execute next entry of replication log. Possible reasons:
    /// 1. Replica is staled, some entries were removed by log cleanup process.
    ///    In this case we should recover replica from the last snapshot.
    /// 2. Replication log is broken due to manual operations with ZooKeeper or logical error.
    ///    In this case we just stop replication without any attempts to recover it automatically,
    ///    because such attempts may lead to unexpected data removal.

    constexpr const char * name = "query-";
    if (!startsWith(entry_name, name))
        throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "Unexpected entry in replication log: {}", entry_name);

    UInt32 entry_number;
    if (!tryParse(entry_number, entry_name.substr(strlen(name))))
        throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "Cannot parse number of replication log entry {}", entry_name);

    if (entry_number < log_entry_to_execute)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Entry {} already executed, current pointer is {}", entry_number, log_entry_to_execute);

    /// Entry name is valid. Let's get min log pointer to check if replica is staled.
    UInt32 min_snapshot = parse<UInt32>(zookeeper->get(zookeeper_path + "/min_log_ptr"));   // FIXME

    if (log_entry_to_execute < min_snapshot)
    {
        recoverLostReplica(zookeeper, 0);   //FIXME log_pointer
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot recover replica, probably it's a bug. "
                                               "Got log entry '{}' when expected entry number {}");
}


BlockIO DatabaseReplicated::propose(const ASTPtr & query, const Context & query_context)
{
    if (query_context.getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "It's not initial query. ON CLUSTER is not allowed for Replicated database.");

    if (auto * ddl_query = query->as<ASTQueryWithTableAndOutput>())
        ddl_query->database.clear();

    if (const auto * query_alter = query->as<ASTAlterQuery>())
    {
        for (const auto & command : query_alter->command_list->children)
        {
            if (!isSupportedAlterType(command->as<ASTAlterCommand&>().type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    LOG_DEBUG(log, "Proposing query: {}", queryToString(query));

    /// TODO maybe write current settings to log entry?
    DDLLogEntry entry;
    entry.query = queryToString(query);
    entry.initiator = ddl_worker->getCommonHostID();
    String node_path = ddl_worker->tryEnqueueAndExecuteEntry(entry, query_context);

    BlockIO io;
    if (query_context.getSettingsRef().distributed_ddl_task_timeout == 0)
        return io;

    Strings hosts_to_wait = getZooKeeper()->getChildren(zookeeper_path + "/replicas");
    auto stream = std::make_shared<DDLQueryStatusInputStream>(node_path, entry, query_context, hosts_to_wait);
    io.in = std::move(stream);
    return io;
}


void DatabaseReplicated::recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 from_snapshot)
{
    //LOG_WARNING(log, "Will recover replica");

    //FIXME drop old tables

    String snapshot_metadata_path = zookeeper_path + "/metadata";
    Strings tables_in_snapshot = current_zookeeper->getChildren(snapshot_metadata_path);
    snapshot_metadata_path += '/';
    from_snapshot = parse<UInt32>(current_zookeeper->get(zookeeper_path + "/max_log_ptr"));

    for (const auto & table_name : tables_in_snapshot)
    {
        //FIXME It's not atomic. We need multiget here (available since ZooKeeper 3.6.0).
        String query_text = current_zookeeper->get(snapshot_metadata_path + table_name);
        auto query_ast = parseQueryFromMetadataInZooKeeper(table_name, query_text);

        Context query_context = global_context;
        query_context.makeQueryContext();
        query_context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        query_context.setCurrentDatabase(database_name);
        query_context.setCurrentQueryId(""); // generate random query_id

        //FIXME
        DatabaseCatalog::instance().waitTableFinallyDropped(query_ast->as<ASTCreateQuery>()->uuid);

        LOG_INFO(log, "Executing {}", serializeAST(*query_ast));
        InterpreterCreateQuery(query_ast, query_context).execute();
    }

    current_zookeeper->set(replica_path + "/log_ptr", toString(from_snapshot));
}

ASTPtr DatabaseReplicated::parseQueryFromMetadataInZooKeeper(const String & node_name, const String & query)
{
    ParserCreateQuery parser;
    String description = "in ZooKeeper " + zookeeper_path + "/metadata/" + node_name;
    auto ast = parseQuery(parser, query, description, 0, global_context.getSettingsRef().max_parser_depth);

    auto & create = ast->as<ASTCreateQuery &>();
    if (create.uuid == UUIDHelpers::Nil || create.table != TABLE_WITH_UUID_NAME_PLACEHOLDER || ! create.database.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected query from {}: {}", node_name, query);

    create.database = getDatabaseName();
    create.table = unescapeForFileName(node_name);
    create.attach = false;

    return ast;
}

void DatabaseReplicated::drop(const Context & context_)
{
    auto current_zookeeper = getZooKeeper();
    current_zookeeper->set(replica_path, DROPPED_MARK);
    DatabaseAtomic::drop(context_);
    current_zookeeper->tryRemoveRecursive(replica_path);
    /// TODO it may leave garbage in ZooKeeper if the last node lost connection here
    if (current_zookeeper->tryRemove(zookeeper_path + "/replicas") == Coordination::Error::ZOK)
    {
        /// It was the last replica, remove all metadata
        current_zookeeper->tryRemoveRecursive(zookeeper_path);
    }
}

void DatabaseReplicated::stopReplication()
{
    if (ddl_worker)
        ddl_worker->shutdown();
}

void DatabaseReplicated::shutdown()
{
    stopReplication();
    ddl_worker = nullptr;
    DatabaseAtomic::shutdown();
}


void DatabaseReplicated::dropTable(const Context & context, const String & table_name, bool no_delay)
{
    auto txn = context.getMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->is_initial_query)
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        txn->ops.emplace_back(zkutil::makeRemoveRequest(metadata_zk_path, -1));
    }
    DatabaseAtomic::dropTable(context, table_name, no_delay);
}

void DatabaseReplicated::renameTable(const Context & context, const String & table_name, IDatabase & to_database,
                                     const String & to_table_name, bool exchange, bool dictionary)
{
    auto txn = context.getMetadataTransaction();
    assert(txn);

    if (txn->is_initial_query)
    {
        if (this != &to_database)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases is not supported for Replicated engine");
        if (table_name == to_table_name)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot rename table to itself");
        if (!isTableExist(table_name, context))
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", table_name);
        if (exchange && !to_database.isTableExist(to_table_name, context))
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", to_table_name);

        String statement;
        String statement_to;
        {
            /// NOTE It's not atomic (however, we have only one thread)
            ReadBufferFromFile in(getObjectMetadataPath(table_name), 4096);
            readStringUntilEOF(statement, in);
            if (exchange)
            {
                ReadBufferFromFile in_to(to_database.getObjectMetadataPath(to_table_name), 4096);
                readStringUntilEOF(statement_to, in_to);
            }
        }
        String metadata_zk_path = txn->zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        String metadata_zk_path_to = txn->zookeeper_path + "/metadata/" + escapeForFileName(to_table_name);
        txn->ops.emplace_back(zkutil::makeRemoveRequest(metadata_zk_path, -1));
        if (exchange)
        {
            txn->ops.emplace_back(zkutil::makeRemoveRequest(metadata_zk_path_to, -1));
            txn->ops.emplace_back(zkutil::makeCreateRequest(metadata_zk_path, statement_to, zkutil::CreateMode::Persistent));
        }
        txn->ops.emplace_back(zkutil::makeCreateRequest(metadata_zk_path_to, statement, zkutil::CreateMode::Persistent));
    }

    DatabaseAtomic::renameTable(context, table_name, to_database, to_table_name, exchange, dictionary);
}

void DatabaseReplicated::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                       const Context & query_context)
{
    auto txn = query_context.getMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->is_initial_query)
    {
        String metadata_zk_path = txn->zookeeper_path + "/metadata/" + escapeForFileName(query.table);
        String statement = getObjectDefinitionFromCreateQuery(query.clone());
        /// zk::multi(...) will throw if `metadata_zk_path` exists
        txn->ops.emplace_back(zkutil::makeCreateRequest(metadata_zk_path, statement, zkutil::CreateMode::Persistent));
    }
    DatabaseAtomic::commitCreateTable(query, table, table_metadata_tmp_path, table_metadata_path, query_context);
}

void DatabaseReplicated::commitAlterTable(const StorageID & table_id,
                                          const String & table_metadata_tmp_path, const String & table_metadata_path,
                                          const String & statement, const Context & query_context)
{
    auto txn = query_context.getMetadataTransaction();
    if (txn && txn->is_initial_query)
    {
        String metadata_zk_path = txn->zookeeper_path + "/metadata/" + escapeForFileName(table_id.table_name);
        txn->ops.emplace_back(zkutil::makeSetRequest(metadata_zk_path, statement, -1));
    }
    DatabaseAtomic::commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path, statement, query_context);
}

void DatabaseReplicated::createDictionary(const Context & context,
                                          const String & dictionary_name,
                                          const ASTPtr & query)
{
    auto txn = context.getMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->is_initial_query)
    {
        String metadata_zk_path = txn->zookeeper_path + "/metadata/" + escapeForFileName(dictionary_name);
        String statement = getObjectDefinitionFromCreateQuery(query->clone());
        txn->ops.emplace_back(zkutil::makeCreateRequest(metadata_zk_path, statement, zkutil::CreateMode::Persistent));
    }
    DatabaseAtomic::createDictionary(context, dictionary_name, query);
}

void DatabaseReplicated::removeDictionary(const Context & context, const String & dictionary_name)
{
    auto txn = context.getMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->is_initial_query)
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(dictionary_name);
        txn->ops.emplace_back(zkutil::makeRemoveRequest(metadata_zk_path, -1));
    }
    DatabaseAtomic::removeDictionary(context, dictionary_name);
}

}
