#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
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
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Common/Macros.h>

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
    extern const int UNKNOWN_TABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}

static constexpr const char * DROPPED_MARK = "DROPPED";
static constexpr const char * BROKEN_TABLES_SUFFIX = "_broken_tables";


zkutil::ZooKeeperPtr DatabaseReplicated::getZooKeeper() const
{
    return getContext()->getZooKeeper();
}

static inline String getHostID(ContextPtr global_context, const UUID & db_uuid)
{
    return Cluster::Address::toString(getFQDNOrHostName(), global_context->getTCPPort()) + ':' + toString(db_uuid);
}


DatabaseReplicated::~DatabaseReplicated() = default;

DatabaseReplicated::DatabaseReplicated(
    const String & name_,
    const String & metadata_path_,
    UUID uuid,
    const String & zookeeper_path_,
    const String & shard_name_,
    const String & replica_name_,
    DatabaseReplicatedSettings db_settings_,
    ContextPtr context_)
    : DatabaseAtomic(name_, metadata_path_, uuid, "DatabaseReplicated (" + name_ + ")", context_)
    , zookeeper_path(zookeeper_path_)
    , shard_name(shard_name_)
    , replica_name(replica_name_)
    , db_settings(std::move(db_settings_))
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
    std::lock_guard lock{mutex};
    if (cluster)
        return cluster;

    cluster = getClusterImpl();
    return cluster;
}

void DatabaseReplicated::setCluster(ClusterPtr && new_cluster)
{
    std::lock_guard lock{mutex};
    cluster = std::move(new_cluster);
}

ClusterPtr DatabaseReplicated::getClusterImpl() const
{
    Strings hosts;
    Strings host_ids;

    auto zookeeper = getContext()->getZooKeeper();
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
        Int32 cversion = stat.cversion;
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
        if (cversion != stat.cversion)
            success = false;
        if (success)
            break;
    }
    if (!success)
        throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Cannot get consistent cluster snapshot,"
                                                                 "because replicas are created or removed concurrently");

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
        auto pos = id.rfind(':');
        String host_port = id.substr(0, pos);
        if (shard != current_shard)
        {
            current_shard = shard;
            if (!shards.back().empty())
                shards.emplace_back();
        }
        shards.back().emplace_back(unescapeForFileName(host_port));
    }

    String username = db_settings.cluster_username;
    String password = db_settings.cluster_password;
    UInt16 default_port = getContext()->getTCPPort();
    bool secure = db_settings.cluster_secure_connection;

    return std::make_shared<Cluster>(getContext()->getSettingsRef(), shards, username, password, default_port, false, secure);
}

void DatabaseReplicated::tryConnectToZooKeeperAndInitDatabase(bool force_attach)
{
    try
    {
        if (!getContext()->hasZooKeeper())
        {
            throw Exception("Can't create replicated database without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
        }

        auto current_zookeeper = getContext()->getZooKeeper();

        if (!current_zookeeper->exists(zookeeper_path))
        {
            /// Create new database, multiple nodes can execute it concurrently
            createDatabaseNodesInZooKeeper(current_zookeeper);
        }

        replica_path = zookeeper_path + "/replicas/" + getFullReplicaName();

        String replica_host_id;
        if (current_zookeeper->tryGet(replica_path, replica_host_id))
        {
            String host_id = getHostID(getContext(), db_uuid);
            if (replica_host_id != host_id)
                throw Exception(ErrorCodes::REPLICA_IS_ALREADY_EXIST,
                                "Replica {} of shard {} of replicated database at {} already exists. Replica host ID: '{}', current host ID: '{}'",
                                replica_name, shard_name, zookeeper_path, replica_host_id, host_id);
        }
        else
        {
            /// Throws if replica with the same name already exists
            createReplicaNodesInZooKeeper(current_zookeeper);
        }

        is_readonly = false;
    }
    catch (...)
    {
        if (!force_attach)
            throw;

        /// It's server startup, ignore error.
        /// Worker thread will try to setup ZooKeeper connection
        tryLogCurrentException(log);
    }
}

bool DatabaseReplicated::createDatabaseNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    current_zookeeper->createAncestors(zookeeper_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/log", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/counter", "", zkutil::CreateMode::Persistent));
    /// We create and remove counter/cnt- node to increment sequential number of counter/ node and make log entry numbers start from 1.
    /// New replicas are created with log pointer equal to 0 and log pointer is a number of the last executed entry.
    /// It means that we cannot have log entry with number 0.
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/counter/cnt-", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/counter/cnt-", -1));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/max_log_ptr", "1", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/logs_to_keep", "1000", zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto res = current_zookeeper->tryMulti(ops, responses);
    if (res == Coordination::Error::ZOK)
        return true;    /// Created new database (it's the first replica)
    if (res == Coordination::Error::ZNODEEXISTS)
        return false;   /// Database exists, we will add new replica

    /// Other codes are unexpected, will throw
    zkutil::KeeperMultiException::check(res, ops, responses);
    assert(false);
    __builtin_unreachable();
}

void DatabaseReplicated::createEmptyLogEntry(Coordination::Requests & ops, const ZooKeeperPtr & current_zookeeper)
{
    /// On replica creation add empty entry to log. Can be used to trigger some actions on other replicas (e.g. update cluster info).
    DDLLogEntry entry{};

    String query_path_prefix = zookeeper_path + "/log/query-";
    String counter_prefix = zookeeper_path + "/counter/cnt-";
    String counter_path = current_zookeeper->create(counter_prefix, "", zkutil::CreateMode::EphemeralSequential);
    String query_path = query_path_prefix + counter_path.substr(counter_prefix.size());

    ops.emplace_back(zkutil::makeCreateRequest(query_path, entry.toString(), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(query_path + "/committed", getFullReplicaName(), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeRemoveRequest(counter_path, -1));
}

void DatabaseReplicated::createReplicaNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    /// Write host name to replica_path, it will protect from multiple replicas with the same name
    auto host_id = getHostID(getContext(), db_uuid);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(replica_path, host_id, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_ptr", "0", zkutil::CreateMode::Persistent));
    createEmptyLogEntry(ops, current_zookeeper);
    current_zookeeper->multi(ops);
}

void DatabaseReplicated::loadStoredObjects(ContextPtr local_context, bool has_force_restore_data_flag, bool force_attach)
{
    tryConnectToZooKeeperAndInitDatabase(force_attach);

    DatabaseAtomic::loadStoredObjects(local_context, has_force_restore_data_flag, force_attach);

    ddl_worker = std::make_unique<DatabaseReplicatedDDLWorker>(this, getContext());
    ddl_worker->startup();
}

void DatabaseReplicated::checkQueryValid(const ASTPtr & query, ContextPtr query_context) const
{
    /// Replicas will set correct name of current database in query context (database name can be different on replicas)
    if (auto * ddl_query = dynamic_cast<ASTQueryWithTableAndOutput *>(query.get()))
    {
        if (ddl_query->database != getDatabaseName())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed");
        ddl_query->database.clear();

        if (auto * create = query->as<ASTCreateQuery>())
        {
            bool replicated_table = create->storage && create->storage->engine && startsWith(create->storage->engine->name, "Replicated");
            if (!replicated_table || !create->storage->engine->arguments)
                return;

            ASTs & args = create->storage->engine->arguments->children;
            if (args.size() < 2)
                return;

            ASTLiteral * arg1 = args[0]->as<ASTLiteral>();
            ASTLiteral * arg2 = args[1]->as<ASTLiteral>();
            if (!arg1 || !arg2 || arg1->value.getType() != Field::Types::String || arg2->value.getType() != Field::Types::String)
                return;

            String maybe_path = arg1->value.get<String>();
            String maybe_replica = arg2->value.get<String>();

            /// Looks like it's ReplicatedMergeTree with explicit zookeeper_path and replica_name arguments.
            /// Let's ensure that some macros are used.
            /// NOTE: we cannot check here that substituted values will be actually different on shards and replicas.

            Macros::MacroExpansionInfo info;
            info.table_id = {getDatabaseName(), create->table, create->uuid};
            query_context->getMacros()->expand(maybe_path, info);
            bool maybe_shard_macros = info.expanded_other;
            info.expanded_other = false;
            query_context->getMacros()->expand(maybe_replica, info);
            bool maybe_replica_macros = info.expanded_other;
            bool enable_functional_tests_helper = getContext()->getConfigRef().has("_functional_tests_helper_database_replicated_replace_args_macros");

            if (!enable_functional_tests_helper)
                LOG_WARNING(log, "It's not recommended to explicitly specify zookeeper_path and replica_name in ReplicatedMergeTree arguments");

            if (maybe_shard_macros && maybe_replica_macros)
                return;

            if (enable_functional_tests_helper)
            {
                if (maybe_path.empty() || maybe_path.back() != '/')
                    maybe_path += '/';
                arg1->value = maybe_path + "auto_{shard}";
                arg2->value = maybe_replica + "auto_{replica}";
                return;
            }

            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "Explicit zookeeper_path and replica_name are specified in ReplicatedMergeTree arguments. "
                            "If you really want to specify it explicitly, then you should use some macros "
                            "to distinguish different shards and replicas");
        }
    }

    if (const auto * query_alter = query->as<ASTAlterQuery>())
    {
        for (const auto & command : query_alter->command_list->children)
        {
            if (!isSupportedAlterType(command->as<ASTAlterCommand&>().type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    if (auto * query_drop = query->as<ASTDropQuery>())
    {
        if (query_drop->kind == ASTDropQuery::Kind::Detach && query_context->getSettingsRef().database_replicated_always_detach_permanently)
            query_drop->permanently = true;
        if (query_drop->kind == ASTDropQuery::Kind::Detach && !query_drop->permanently)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "DETACH TABLE is not allowed for Replicated databases. "
                                                         "Use DETACH TABLE PERMANENTLY or SYSTEM RESTART REPLICA or set "
                                                         "database_replicated_always_detach_permanently to 1");
    }
}

BlockIO DatabaseReplicated::tryEnqueueReplicatedDDL(const ASTPtr & query, ContextPtr query_context)
{
    if (is_readonly)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "Database is in readonly mode, because it cannot connect to ZooKeeper");

    if (query_context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "It's not initial query. ON CLUSTER is not allowed for Replicated database.");

    checkQueryValid(query, query_context);
    LOG_DEBUG(log, "Proposing query: {}", queryToString(query));

    DDLLogEntry entry;
    entry.query = queryToString(query);
    entry.initiator = ddl_worker->getCommonHostID();
    entry.setSettingsIfRequired(query_context);
    String node_path = ddl_worker->tryEnqueueAndExecuteEntry(entry, query_context);

    Strings hosts_to_wait = getZooKeeper()->getChildren(zookeeper_path + "/replicas");
    return getDistributedDDLStatus(node_path, entry, query_context, hosts_to_wait);
}

static UUID getTableUUIDIfReplicated(const String & metadata, ContextPtr context)
{
    bool looks_like_replicated = metadata.find("ReplicatedMergeTree") != std::string::npos;
    if (!looks_like_replicated)
        return UUIDHelpers::Nil;

    ParserCreateQuery parser;
    auto size = context->getSettingsRef().max_query_size;
    auto depth = context->getSettingsRef().max_parser_depth;
    ASTPtr query = parseQuery(parser, metadata, size, depth);
    const ASTCreateQuery & create = query->as<const ASTCreateQuery &>();
    if (!create.storage || !create.storage->engine)
        return UUIDHelpers::Nil;
    if (!startsWith(create.storage->engine->name, "Replicated") || !endsWith(create.storage->engine->name, "MergeTree"))
        return UUIDHelpers::Nil;
    assert(create.uuid != UUIDHelpers::Nil);
    return create.uuid;
}

void DatabaseReplicated::recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 max_log_ptr)
{
    /// Let's compare local (possibly outdated) metadata with (most actual) metadata stored in ZooKeeper
    /// and try to update the set of local tables.
    /// We could drop all local tables and create the new ones just like it's new replica.
    /// But it will cause all ReplicatedMergeTree tables to fetch all data parts again and data in other tables will be lost.

    bool new_replica = our_log_ptr == 0;
    if (new_replica)
        LOG_INFO(log, "Will create new replica from log pointer {}", max_log_ptr);
    else
        LOG_WARNING(log, "Will recover replica with staled log pointer {} from log pointer {}", our_log_ptr, max_log_ptr);

    if (new_replica && !empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "It's new replica, but database is not empty");

    auto table_name_to_metadata = tryGetConsistentMetadataSnapshot(current_zookeeper, max_log_ptr);

    /// For ReplicatedMergeTree tables we can compare only UUIDs to ensure that it's the same table.
    /// Metadata can be different, it's handled on table replication level.
    /// We need to handle renamed tables only.
    /// TODO maybe we should also update MergeTree SETTINGS if required?
    std::unordered_map<UUID, String> zk_replicated_id_to_name;
    for (const auto & zk_table : table_name_to_metadata)
    {
        UUID zk_replicated_id = getTableUUIDIfReplicated(zk_table.second, getContext());
        if (zk_replicated_id != UUIDHelpers::Nil)
            zk_replicated_id_to_name.emplace(zk_replicated_id, zk_table.first);
    }

    /// We will drop or move tables which exist only in local metadata
    Strings tables_to_detach;
    std::vector<std::pair<String, String>> replicated_tables_to_rename;
    size_t total_tables = 0;
    std::vector<UUID> replicated_ids;
    for (auto existing_tables_it = getTablesIterator(getContext(), {}); existing_tables_it->isValid();
         existing_tables_it->next(), ++total_tables)
    {
        String name = existing_tables_it->name();
        UUID local_replicated_id = UUIDHelpers::Nil;
        if (existing_tables_it->table()->supportsReplication())
        {
            /// Check if replicated tables have the same UUID
            local_replicated_id = existing_tables_it->table()->getStorageID().uuid;
            auto it = zk_replicated_id_to_name.find(local_replicated_id);
            if (it != zk_replicated_id_to_name.end())
            {
                if (name != it->second)
                {
                    /// Need just update table name
                    replicated_tables_to_rename.emplace_back(name, it->second);
                }
                continue;
            }
        }

        auto in_zk = table_name_to_metadata.find(name);
        if (in_zk == table_name_to_metadata.end() || in_zk->second != readMetadataFile(name))
        {
            /// Local table does not exits in ZooKeeper or has different metadata
            tables_to_detach.emplace_back(std::move(name));
        }
    }

    String db_name = getDatabaseName();
    String to_db_name = getDatabaseName() + BROKEN_TABLES_SUFFIX;
    if (total_tables * db_settings.max_broken_tables_ratio < tables_to_detach.size())
        throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "Too many tables to recreate: {} of {}", tables_to_detach.size(), total_tables);
    else if (!tables_to_detach.empty())
    {
        LOG_WARNING(log, "Will recreate {} broken tables to recover replica", tables_to_detach.size());
        /// It's too dangerous to automatically drop tables, so we will move them to special database.
        /// We use Ordinary engine for destination database, because it's the only way to discard table UUID
        /// and make possible creation of new table with the same UUID.
        String query = fmt::format("CREATE DATABASE IF NOT EXISTS {} ENGINE=Ordinary", backQuoteIfNeed(to_db_name));
        auto query_context = Context::createCopy(getContext());
        executeQuery(query, query_context, true);
    }

    size_t moved_tables = 0;
    std::vector<UUID> dropped_tables;
    size_t dropped_dictionaries = 0;

    for (const auto & table_name : tables_to_detach)
    {
        DDLGuardPtr table_guard = DatabaseCatalog::instance().getDDLGuard(db_name, table_name);
        if (getDatabaseName() != db_name)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed, will retry");

        auto table = tryGetTable(table_name, getContext());

        if (!table->storesDataOnDisk())
        {
            LOG_DEBUG(log, "Will DROP TABLE {}, because it does not store data on disk and can be safely dropped", backQuoteIfNeed(table_name));
            dropped_tables.push_back(tryGetTableUUID(table_name));
            dropped_dictionaries += table->isDictionary();

            table->shutdown();
            DatabaseAtomic::dropTable(getContext(), table_name, true);
        }
        else
        {
            /// Table probably stores some data. Let's move it to another database.
            String to_name = fmt::format("{}_{}_{}", table_name, max_log_ptr, thread_local_rng() % 1000);
            LOG_DEBUG(log, "Will RENAME TABLE {} TO {}.{}", backQuoteIfNeed(table_name), backQuoteIfNeed(to_db_name), backQuoteIfNeed(to_name));
            assert(db_name < to_db_name);
            DDLGuardPtr to_table_guard = DatabaseCatalog::instance().getDDLGuard(to_db_name, to_name);
            auto to_db_ptr = DatabaseCatalog::instance().getDatabase(to_db_name);
            DatabaseAtomic::renameTable(getContext(), table_name, *to_db_ptr, to_name, false, false);
            ++moved_tables;
        }
    }

    if (!tables_to_detach.empty())
        LOG_WARNING(log, "Cleaned {} outdated objects: dropped {} dictionaries and {} tables, moved {} tables",
                    tables_to_detach.size(), dropped_dictionaries, dropped_tables.size() - dropped_dictionaries, moved_tables);

    /// Now database is cleared from outdated tables, let's rename ReplicatedMergeTree tables to actual names
    for (const auto & old_to_new : replicated_tables_to_rename)
    {
        const String & from = old_to_new.first;
        const String & to = old_to_new.second;

        LOG_DEBUG(log, "Will RENAME TABLE {} TO {}", backQuoteIfNeed(from), backQuoteIfNeed(to));
        /// TODO Maybe we should do it in two steps: rename all tables to temporary names and then rename them to actual names?
        DDLGuardPtr table_guard = DatabaseCatalog::instance().getDDLGuard(db_name, std::min(from, to));
        DDLGuardPtr to_table_guard = DatabaseCatalog::instance().getDDLGuard(db_name, std::max(from, to));
        DatabaseAtomic::renameTable(getContext(), from, *this, to, false, false);
    }

    for (const auto & id : dropped_tables)
        DatabaseCatalog::instance().waitTableFinallyDropped(id);

    for (const auto & name_and_meta : table_name_to_metadata)
    {
        if (isTableExist(name_and_meta.first, getContext()))
        {
            assert(name_and_meta.second == readMetadataFile(name_and_meta.first));
            continue;
        }

        auto query_ast = parseQueryFromMetadataInZooKeeper(name_and_meta.first, name_and_meta.second);

        auto query_context = Context::createCopy(getContext());
        query_context->makeQueryContext();
        query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        query_context->setCurrentDatabase(database_name);
        query_context->setCurrentQueryId(""); // generate random query_id

        LOG_INFO(log, "Executing {}", serializeAST(*query_ast));
        InterpreterCreateQuery(query_ast, query_context).execute();
    }

    current_zookeeper->set(replica_path + "/log_ptr", toString(max_log_ptr));
}

std::map<String, String> DatabaseReplicated::tryGetConsistentMetadataSnapshot(const ZooKeeperPtr & zookeeper, UInt32 & max_log_ptr)
{
    std::map<String, String> table_name_to_metadata;
    constexpr int max_retries = 10;
    int iteration = 0;
    while (++iteration <= max_retries)
    {
        table_name_to_metadata.clear();
        LOG_DEBUG(log, "Trying to get consistent metadata snapshot for log pointer {}", max_log_ptr);
        Strings table_names = zookeeper->getChildren(zookeeper_path + "/metadata");

        std::vector<zkutil::ZooKeeper::FutureGet> futures;
        futures.reserve(table_names.size());
        for (const auto & table : table_names)
            futures.emplace_back(zookeeper->asyncTryGet(zookeeper_path + "/metadata/" + table));

        for (size_t i = 0; i < table_names.size(); ++i)
        {
            auto res = futures[i].get();
            if (res.error != Coordination::Error::ZOK)
                break;
            table_name_to_metadata.emplace(unescapeForFileName(table_names[i]), res.data);
        }

        UInt32 new_max_log_ptr = parse<UInt32>(zookeeper->get(zookeeper_path + "/max_log_ptr"));
        if (new_max_log_ptr == max_log_ptr && table_names.size() == table_name_to_metadata.size())
            break;

        if (max_log_ptr < new_max_log_ptr)
        {
            LOG_DEBUG(log, "Log pointer moved from {} to {}, will retry", max_log_ptr, new_max_log_ptr);
            max_log_ptr = new_max_log_ptr;
        }
        else
        {
            assert(max_log_ptr == new_max_log_ptr);
            assert(table_names.size() != table_name_to_metadata.size());
            LOG_DEBUG(log, "Cannot get metadata of some tables due to ZooKeeper error, will retry");
        }
    }

    if (max_retries < iteration)
        throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "Cannot get consistent metadata snapshot");

    LOG_DEBUG(log, "Got consistent metadata snapshot for log pointer {}", max_log_ptr);

    return table_name_to_metadata;
}

ASTPtr DatabaseReplicated::parseQueryFromMetadataInZooKeeper(const String & node_name, const String & query)
{
    ParserCreateQuery parser;
    String description = "in ZooKeeper " + zookeeper_path + "/metadata/" + node_name;
    auto ast = parseQuery(parser, query, description, 0, getContext()->getSettingsRef().max_parser_depth);

    auto & create = ast->as<ASTCreateQuery &>();
    if (create.uuid == UUIDHelpers::Nil || create.table != TABLE_WITH_UUID_NAME_PLACEHOLDER || !create.database.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected query from {}: {}", node_name, query);

    bool is_materialized_view_with_inner_table = create.is_materialized_view && create.to_table_id.empty();

    create.database = getDatabaseName();
    create.table = unescapeForFileName(node_name);
    create.attach = is_materialized_view_with_inner_table;

    return ast;
}

void DatabaseReplicated::drop(ContextPtr context_)
{
    auto current_zookeeper = getZooKeeper();
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeSetRequest(replica_path, DROPPED_MARK, -1));
    createEmptyLogEntry(ops, current_zookeeper);
    current_zookeeper->multi(ops);

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


void DatabaseReplicated::dropTable(ContextPtr local_context, const String & table_name, bool no_delay)
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn || startsWith(table_name, ".inner_id."));
    if (txn && txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));
    }
    DatabaseAtomic::dropTable(local_context, table_name, no_delay);
}

void DatabaseReplicated::renameTable(ContextPtr local_context, const String & table_name, IDatabase & to_database,
                                     const String & to_table_name, bool exchange, bool dictionary)
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(txn);

    if (txn->isInitialQuery())
    {
        if (this != &to_database)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases is not supported for Replicated engine");
        if (table_name == to_table_name)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot rename table to itself");
        if (!isTableExist(table_name, local_context))
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", table_name);
        if (exchange && !to_database.isTableExist(to_table_name, local_context))
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", to_table_name);

        String statement = readMetadataFile(table_name);
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        String metadata_zk_path_to = zookeeper_path + "/metadata/" + escapeForFileName(to_table_name);
        txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));
        if (exchange)
        {
            String statement_to = readMetadataFile(to_table_name);
            txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path_to, -1));
            txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, statement_to, zkutil::CreateMode::Persistent));
        }
        txn->addOp(zkutil::makeCreateRequest(metadata_zk_path_to, statement, zkutil::CreateMode::Persistent));
    }

    DatabaseAtomic::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
}

void DatabaseReplicated::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                       ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(query.table);
        String statement = getObjectDefinitionFromCreateQuery(query.clone());
        /// zk::multi(...) will throw if `metadata_zk_path` exists
        txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, statement, zkutil::CreateMode::Persistent));
    }
    DatabaseAtomic::commitCreateTable(query, table, table_metadata_tmp_path, table_metadata_path, query_context);
}

void DatabaseReplicated::commitAlterTable(const StorageID & table_id,
                                          const String & table_metadata_tmp_path, const String & table_metadata_path,
                                          const String & statement, ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();
    if (txn && txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_id.table_name);
        txn->addOp(zkutil::makeSetRequest(metadata_zk_path, statement, -1));
    }
    DatabaseAtomic::commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path, statement, query_context);
}

void DatabaseReplicated::detachTablePermanently(ContextPtr local_context, const String & table_name)
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->isInitialQuery())
    {
        /// We have to remove metadata from zookeeper, because we do not distinguish permanently detached tables
        /// from attached tables when recovering replica.
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));
    }
    DatabaseAtomic::detachTablePermanently(local_context, table_name);
}

void DatabaseReplicated::removeDetachedPermanentlyFlag(ContextPtr local_context, const String & table_name, const String & table_metadata_path, bool attach) const
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->isInitialQuery() && attach)
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        String statement = readMetadataFile(table_name);
        txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, statement, zkutil::CreateMode::Persistent));
    }
    DatabaseAtomic::removeDetachedPermanentlyFlag(local_context, table_name, table_metadata_path, attach);
}


String DatabaseReplicated::readMetadataFile(const String & table_name) const
{
    String statement;
    ReadBufferFromFile in(getObjectMetadataPath(table_name), 4096);
    readStringUntilEOF(statement, in);
    return statement;
}

}
