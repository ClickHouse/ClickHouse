#include <DataTypes/DataTypeString.h>

#include <utility>

#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <base/chrono_io.h>
#include <base/getFQDNOrHostName.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/DatabaseReplicatedWorker.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/TablesDependencyGraph.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageKeeperMap.h>
#include <Storages/AlterCommands.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int REPLICA_ALREADY_EXISTS;
    extern const int DATABASE_REPLICATION_FAILED;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int INCONSISTENT_METADATA_FOR_BACKUP;
    extern const int CANNOT_RESTORE_TABLE;
}

static constexpr const char * REPLICATED_DATABASE_MARK = "DatabaseReplicated";
static constexpr const char * DROPPED_MARK = "DROPPED";
static constexpr const char * BROKEN_TABLES_SUFFIX = "_broken_tables";
static constexpr const char * BROKEN_REPLICATED_TABLES_SUFFIX = "_broken_replicated_tables";

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

zkutil::ZooKeeperPtr DatabaseReplicated::getZooKeeper() const
{
    return getContext()->getZooKeeper();
}

static inline String getHostID(ContextPtr global_context, const UUID & db_uuid)
{
    return Cluster::Address::toString(getFQDNOrHostName(), global_context->getTCPPort()) + ':' + toString(db_uuid);
}

static inline UInt64 getMetadataHash(const String & table_name, const String & metadata)
{
    SipHash hash;
    hash.update(table_name);
    hash.update(metadata);
    return hash.get64();
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
    , tables_metadata_digest(0)
{
    if (zookeeper_path.empty() || shard_name.empty() || replica_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path, shard and replica names must be non-empty");
    if (shard_name.find('/') != std::string::npos || replica_name.find('/') != std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard and replica names should not contain '/'");
    if (shard_name.find('|') != std::string::npos || replica_name.find('|') != std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard and replica names should not contain '|'");

    if (zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);

    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    if (!db_settings.collection_name.value.empty())
        fillClusterAuthInfo(db_settings.collection_name.value, context_->getConfigRef());
}

String DatabaseReplicated::getFullReplicaName(const String & shard, const String & replica)
{
    return shard + '|' + replica;
}

String DatabaseReplicated::getFullReplicaName() const
{
    return getFullReplicaName(shard_name, replica_name);
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

ClusterPtr DatabaseReplicated::tryGetCluster() const
{
    std::lock_guard lock{mutex};
    if (cluster)
        return cluster;

    /// Database is probably not created or not initialized yet, it's ok to return nullptr
    if (is_readonly)
        return cluster;

    try
    {
        /// A quick fix for stateless tests with DatabaseReplicated. Its ZK
        /// node can be destroyed at any time. If another test lists
        /// system.clusters to get client command line suggestions, it will
        /// get an error when trying to get the info about DB from ZK.
        /// Just ignore these inaccessible databases. A good example of a
        /// failing test is `01526_client_start_and_exit`.
        cluster = getClusterImpl();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
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
            throw Exception(ErrorCodes::NO_ACTIVE_REPLICAS, "No replicas of database {} found. "
                            "It's possible if the first replica is not fully created yet "
                            "or if the last replica was just dropped or due to logical error", zookeeper_path);
        Int32 cversion = stat.cversion;
        ::sort(hosts.begin(), hosts.end());

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
    std::vector<std::vector<DatabaseReplicaInfo>> shards;
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
        String hostname = unescapeForFileName(host_port);
        shards.back().push_back(DatabaseReplicaInfo{std::move(hostname), std::move(shard), std::move(replica)});
    }

    UInt16 default_port = getContext()->getTCPPort();

    bool treat_local_as_remote = false;
    bool treat_local_port_as_remote = getContext()->getApplicationType() == Context::ApplicationType::LOCAL;
    ClusterConnectionParameters params{
        cluster_auth_info.cluster_username,
        cluster_auth_info.cluster_password,
        default_port,
        treat_local_as_remote,
        treat_local_port_as_remote,
        cluster_auth_info.cluster_secure_connection,
        Priority{1},
        TSA_SUPPRESS_WARNING_FOR_READ(database_name),     /// FIXME
        cluster_auth_info.cluster_secret};

    return std::make_shared<Cluster>(getContext()->getSettingsRef(), shards, params);
}

std::vector<UInt8> DatabaseReplicated::tryGetAreReplicasActive(const ClusterPtr & cluster_) const
{
    Strings paths;
    const auto & addresses_with_failover = cluster_->getShardsAddresses();
    const auto & shards_info = cluster_->getShardsInfo();
    for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
    {
        for (const auto & replica : addresses_with_failover[shard_index])
        {
            String full_name = getFullReplicaName(replica.database_shard_name, replica.database_replica_name);
            paths.emplace_back(fs::path(zookeeper_path) / "replicas" / full_name / "active");
        }
    }

    try
    {
        auto current_zookeeper = getZooKeeper();
        auto res = current_zookeeper->exists(paths);

        std::vector<UInt8> statuses;
        statuses.resize(paths.size());

        for (size_t i = 0; i < res.size(); ++i)
            if (res[i].error == Coordination::Error::ZOK)
                statuses[i] = 1;

        return statuses;
    }
    catch (...)
    {
        tryLogCurrentException(log);
        return {};
    }
}


void DatabaseReplicated::fillClusterAuthInfo(String collection_name, const Poco::Util::AbstractConfiguration & config_ref)
{
    const auto & config_prefix = fmt::format("named_collections.{}", collection_name);

    if (!config_ref.has(config_prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection_name);

    cluster_auth_info.cluster_username = config_ref.getString(config_prefix + ".cluster_username", "");
    cluster_auth_info.cluster_password = config_ref.getString(config_prefix + ".cluster_password", "");
    cluster_auth_info.cluster_secret = config_ref.getString(config_prefix + ".cluster_secret", "");
    cluster_auth_info.cluster_secure_connection = config_ref.getBool(config_prefix + ".cluster_secure_connection", false);
}

void DatabaseReplicated::tryConnectToZooKeeperAndInitDatabase(LoadingStrictnessLevel mode)
{
    try
    {
        if (!getContext()->hasZooKeeper())
        {
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Can't create replicated database without ZooKeeper");
        }

        auto current_zookeeper = getContext()->getZooKeeper();

        if (!current_zookeeper->exists(zookeeper_path))
        {
            /// Create new database, multiple nodes can execute it concurrently
            createDatabaseNodesInZooKeeper(current_zookeeper);
        }

        replica_path = fs::path(zookeeper_path) / "replicas" / getFullReplicaName();
        bool is_create_query = mode == LoadingStrictnessLevel::CREATE;

        String replica_host_id;
        if (current_zookeeper->tryGet(replica_path, replica_host_id))
        {
            if (replica_host_id == DROPPED_MARK && !is_create_query)
            {
                LOG_WARNING(log, "Database {} exists locally, but marked dropped in ZooKeeper ({}). "
                                 "Will not try to start it up", getDatabaseName(), replica_path);
                is_probably_dropped = true;
                return;
            }

            String host_id = getHostID(getContext(), db_uuid);
            if (is_create_query || replica_host_id != host_id)
            {
                throw Exception(
                    ErrorCodes::REPLICA_ALREADY_EXISTS,
                    "Replica {} of shard {} of replicated database at {} already exists. Replica host ID: '{}', current host ID: '{}'",
                    replica_name, shard_name, zookeeper_path, replica_host_id, host_id);
            }
        }
        else if (is_create_query)
        {
            /// Create new replica. Throws if replica with the same name already exists
            createReplicaNodesInZooKeeper(current_zookeeper);
        }
        else
        {
            /// It's not CREATE query, but replica does not exist. Probably it was dropped.
            /// Do not create anything, continue as readonly.
            LOG_WARNING(log, "Database {} exists locally, but its replica does not exist in ZooKeeper ({}). "
                             "Assuming it was dropped, will not try to start it up", getDatabaseName(), replica_path);
            is_probably_dropped = true;
            return;
        }

        is_readonly = false;
    }
    catch (...)
    {
        if (mode < LoadingStrictnessLevel::FORCE_ATTACH)
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
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, REPLICATED_DATABASE_MARK, zkutil::CreateMode::Persistent));
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
    UNREACHABLE();
}

bool DatabaseReplicated::looksLikeReplicatedDatabasePath(const ZooKeeperPtr & current_zookeeper, const String & path)
{
    Coordination::Stat stat;
    String maybe_database_mark;
    if (!current_zookeeper->tryGet(path, maybe_database_mark, &stat))
        return false;
    if (maybe_database_mark.starts_with(REPLICATED_DATABASE_MARK))
        return true;
    if (!maybe_database_mark.empty())
        return false;

    /// Old versions did not have REPLICATED_DATABASE_MARK. Check specific nodes exist and add mark.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCheckRequest(path + "/log", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/replicas", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/counter", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/metadata", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/max_log_ptr", -1));
    ops.emplace_back(zkutil::makeCheckRequest(path + "/logs_to_keep", -1));
    ops.emplace_back(zkutil::makeSetRequest(path, REPLICATED_DATABASE_MARK, stat.version));
    Coordination::Responses responses;
    auto res = current_zookeeper->tryMulti(ops, responses);
    if (res == Coordination::Error::ZOK)
        return true;

    /// Recheck database mark (just in case of concurrent update).
    if (!current_zookeeper->tryGet(path, maybe_database_mark, &stat))
        return false;

    return maybe_database_mark.starts_with(REPLICATED_DATABASE_MARK);
}

void DatabaseReplicated::createEmptyLogEntry(const ZooKeeperPtr & current_zookeeper)
{
    /// On replica creation add empty entry to log. Can be used to trigger some actions on other replicas (e.g. update cluster info).
    DDLLogEntry entry{};
    DatabaseReplicatedDDLWorker::enqueueQueryImpl(current_zookeeper, entry, this, true);
}

bool DatabaseReplicated::waitForReplicaToProcessAllEntries(UInt64 timeout_ms)
{
    if (!ddl_worker || is_probably_dropped)
        return false;
    return ddl_worker->waitForReplicaToProcessAllEntries(timeout_ms);
}

void DatabaseReplicated::createReplicaNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    if (!looksLikeReplicatedDatabasePath(current_zookeeper, zookeeper_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot add new database replica: provided path {} "
                        "already contains some data and it does not look like Replicated database path.", zookeeper_path);

    /// Write host name to replica_path, it will protect from multiple replicas with the same name
    auto host_id = getHostID(getContext(), db_uuid);

    for (int attempts = 10; attempts > 0; --attempts)
    {
        Coordination::Stat stat;
        String max_log_ptr_str = current_zookeeper->get(zookeeper_path + "/max_log_ptr", &stat);
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, host_id, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/log_ptr", "0", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/digest", "0", zkutil::CreateMode::Persistent));
        /// In addition to creating the replica nodes, we record the max_log_ptr at the instant where
        /// we declared ourself as an existing replica. We'll need this during recoverLostReplica to
        /// notify other nodes that issued new queries while this node was recovering.
        ops.emplace_back(zkutil::makeCheckRequest(zookeeper_path + "/max_log_ptr", stat.version));
        Coordination::Responses responses;
        const auto code = current_zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZOK)
        {
            max_log_ptr_at_creation = parse<UInt32>(max_log_ptr_str);
            break;
        }
        else if (code == Coordination::Error::ZNODEEXISTS || attempts == 1)
        {
            /// If its our last attempt, or if the replica already exists, fail immediately.
            zkutil::KeeperMultiException::check(code, ops, responses);
        }
    }
    createEmptyLogEntry(current_zookeeper);
}

void DatabaseReplicated::beforeLoadingMetadata(ContextMutablePtr /*context*/, LoadingStrictnessLevel mode)
{
    tryConnectToZooKeeperAndInitDatabase(mode);
}

void DatabaseReplicated::loadStoredObjects(ContextMutablePtr local_context, LoadingStrictnessLevel mode)
{
    beforeLoadingMetadata(local_context, mode);
    DatabaseAtomic::loadStoredObjects(local_context, mode);
}

UInt64 DatabaseReplicated::getMetadataHash(const String & table_name) const
{
    return DB::getMetadataHash(table_name, readMetadataFile(table_name));
}

void DatabaseReplicated::startupTables(ThreadPool & thread_pool, LoadingStrictnessLevel mode)
{
    DatabaseAtomic::startupTables(thread_pool, mode);

    /// TSA: No concurrent writes are possible during loading
    UInt64 digest = 0;
    for (const auto & table : TSA_SUPPRESS_WARNING_FOR_READ(tables))
        digest += getMetadataHash(table.first);

    LOG_DEBUG(log, "Calculated metadata digest of {} tables: {}", TSA_SUPPRESS_WARNING_FOR_READ(tables).size(), digest);
    chassert(!TSA_SUPPRESS_WARNING_FOR_READ(tables_metadata_digest));
    TSA_SUPPRESS_WARNING_FOR_WRITE(tables_metadata_digest) = digest;

    if (is_probably_dropped)
        return;

    ddl_worker = std::make_unique<DatabaseReplicatedDDLWorker>(this, getContext());
    ddl_worker->startup();
    ddl_worker_initialized = true;
}

bool DatabaseReplicated::checkDigestValid(const ContextPtr & local_context, bool debug_check /* = true */) const
{
    if (debug_check)
    {
        /// Reduce number of debug checks
        if (thread_local_rng() % 16)
            return true;
    }

    LOG_TEST(log, "Current in-memory metadata digest: {}", tables_metadata_digest);

    /// Database is probably being dropped
    if (!local_context->getZooKeeperMetadataTransaction() && (!ddl_worker || !ddl_worker->isCurrentlyActive()))
        return true;

    UInt64 local_digest = 0;
    {
        std::lock_guard lock{mutex};
        for (const auto & table : TSA_SUPPRESS_WARNING_FOR_READ(tables))
            local_digest += getMetadataHash(table.first);
    }

    if (local_digest != tables_metadata_digest)
    {
        LOG_ERROR(log, "Digest of local metadata ({}) is not equal to in-memory digest ({})", local_digest, tables_metadata_digest);
        return false;
    }

    /// Do not check digest in Keeper after internal subquery, it's probably not committed yet
    if (local_context->isInternalSubquery())
        return true;

    /// Check does not make sense to check digest in Keeper during recovering
    if (is_recovering)
        return true;

    String zk_digest = getZooKeeper()->get(replica_path + "/digest");
    String local_digest_str = toString(local_digest);
    if (zk_digest != local_digest_str)
    {
        LOG_ERROR(log, "Digest of local metadata ({}) is not equal to digest in Keeper ({})", local_digest_str, zk_digest);
        return false;
    }

    return true;
}

void DatabaseReplicated::checkQueryValid(const ASTPtr & query, ContextPtr query_context) const
{
    /// Replicas will set correct name of current database in query context (database name can be different on replicas)
    if (auto * ddl_query = dynamic_cast<ASTQueryWithTableAndOutput *>(query.get()))
    {
        if (ddl_query->getDatabase() != getDatabaseName())
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed");
        ddl_query->database.reset();

        if (auto * create = query->as<ASTCreateQuery>())
        {
            bool replicated_table = create->storage && create->storage->engine && startsWith(create->storage->engine->name, "Replicated");
            if (!replicated_table || !create->storage->engine->arguments)
                return;

            ASTs & args_ref = create->storage->engine->arguments->children;
            ASTs args = args_ref;
            if (args.size() < 2)
                return;

            /// It can be a constant expression. Try to evaluate it, ignore exception if we cannot.
            bool has_expression_argument = args_ref[0]->as<ASTFunction>() || args_ref[1]->as<ASTFunction>();
            if (has_expression_argument)
            {
                try
                {
                    args[0] = evaluateConstantExpressionAsLiteral(args_ref[0]->clone(), query_context);
                    args[1] = evaluateConstantExpressionAsLiteral(args_ref[1]->clone(), query_context);
                }
                catch (...)
                {
                }
            }

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
            info.table_id = {getDatabaseName(), create->getTable(), create->uuid};
            info.shard = getShardName();
            info.replica = getReplicaName();
            query_context->getMacros()->expand(maybe_path, info);
            bool maybe_shard_macros = info.expanded_other;
            info.expanded_other = false;
            query_context->getMacros()->expand(maybe_replica, info);
            bool maybe_replica_macros = info.expanded_other;
            bool enable_functional_tests_helper = getContext()->getConfigRef().has("_functional_tests_helper_database_replicated_replace_args_macros");

            if (!enable_functional_tests_helper)
            {
                if (query_context->getSettingsRef().database_replicated_allow_replicated_engine_arguments)
                    LOG_WARNING(log, "It's not recommended to explicitly specify zookeeper_path and replica_name in ReplicatedMergeTree arguments");
                else
                    throw Exception(ErrorCodes::INCORRECT_QUERY,
                                    "It's not allowed to specify explicit zookeeper_path and replica_name "
                                    "for ReplicatedMergeTree arguments in Replicated database. If you really want to "
                                    "specify them explicitly, enable setting "
                                    "database_replicated_allow_replicated_engine_arguments.");
            }

            if (maybe_shard_macros && maybe_replica_macros)
                return;

            if (enable_functional_tests_helper && !has_expression_argument)
            {
                if (maybe_path.empty() || maybe_path.back() != '/')
                    maybe_path += '/';
                args_ref[0]->as<ASTLiteral>()->value = maybe_path + "auto_{shard}";
                args_ref[1]->as<ASTLiteral>()->value = maybe_replica + "auto_{replica}";
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
            if (!isSupportedAlterTypeForOnClusterDDLQuery(command->as<ASTAlterCommand&>().type))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type of ALTER query");
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

BlockIO DatabaseReplicated::tryEnqueueReplicatedDDL(const ASTPtr & query, ContextPtr query_context, bool internal)
{

    if (query_context->getCurrentTransaction() && query_context->getSettingsRef().throw_on_unsupported_query_inside_transaction)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Distributed DDL queries inside transactions are not supported");

    if (is_readonly)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "Database is in readonly mode, because it cannot connect to ZooKeeper");

    if (!internal && (query_context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "It's not initial query. ON CLUSTER is not allowed for Replicated database.");

    checkQueryValid(query, query_context);
    LOG_DEBUG(log, "Proposing query: {}", queryToString(query));

    DDLLogEntry entry;
    entry.query = queryToString(query);
    entry.initiator = ddl_worker->getCommonHostID();
    entry.setSettingsIfRequired(query_context);
    entry.tracing_context = OpenTelemetry::CurrentContext();
    String node_path = ddl_worker->tryEnqueueAndExecuteEntry(entry, query_context);

    Strings hosts_to_wait = getZooKeeper()->getChildren(zookeeper_path + "/replicas");
    return getDistributedDDLStatus(node_path, entry, query_context, &hosts_to_wait);
}

static UUID getTableUUIDIfReplicated(const String & metadata, ContextPtr context)
{
    bool looks_like_replicated = metadata.find("Replicated") != std::string::npos;
    bool looks_like_merge_tree = metadata.find("MergeTree") != std::string::npos;
    if (!looks_like_replicated || !looks_like_merge_tree)
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
    chassert(create.uuid != UUIDHelpers::Nil);
    return create.uuid;
}

void DatabaseReplicated::recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 & max_log_ptr)
{
    is_recovering = true;
    SCOPE_EXIT({ is_recovering = false; });

    /// Let's compare local (possibly outdated) metadata with (most actual) metadata stored in ZooKeeper
    /// and try to update the set of local tables.
    /// We could drop all local tables and create the new ones just like it's new replica.
    /// But it will cause all ReplicatedMergeTree tables to fetch all data parts again and data in other tables will be lost.

    bool new_replica = our_log_ptr == 0;
    if (new_replica)
        LOG_INFO(log, "Will create new replica from log pointer {}", max_log_ptr);
    else
        LOG_WARNING(log, "Will recover replica with staled log pointer {} from log pointer {}", our_log_ptr, max_log_ptr);

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

    struct RenameEdge
    {
        String from;
        String intermediate;
        String to;
    };

    /// This is needed to generate intermediate name
    String salt = toString(thread_local_rng());

    std::vector<RenameEdge> replicated_tables_to_rename;
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
                    String intermediate_name;
                    /// Possibly we failed to rename it on previous iteration
                    /// And this table was already renamed to an intermediate name
                    if (startsWith(name, ".rename-") && !startsWith(it->second, ".rename-"))
                        intermediate_name = name;
                    else
                        intermediate_name = fmt::format(".rename-{}-{}", name, sipHash64(fmt::format("{}-{}", name, salt)));
                    /// Need just update table name
                    replicated_tables_to_rename.push_back({name, intermediate_name, it->second});
                }
                continue;
            }
        }

        auto in_zk = table_name_to_metadata.find(name);
        if (in_zk == table_name_to_metadata.end() || in_zk->second != readMetadataFile(name))
        {
            /// Local table does not exist in ZooKeeper or has different metadata
            tables_to_detach.emplace_back(std::move(name));
        }
    }

    auto make_query_context = [this, current_zookeeper]()
    {
        auto query_context = Context::createCopy(getContext());
        query_context->makeQueryContext();
        query_context->setQueryKind(ClientInfo::QueryKind::SECONDARY_QUERY);
        query_context->setQueryKindReplicatedDatabaseInternal();
        query_context->setCurrentDatabase(getDatabaseName());
        query_context->setCurrentQueryId("");

        /// We will execute some CREATE queries for recovery (not ATTACH queries),
        /// so we need to allow experimental features that can be used in a CREATE query
        query_context->setSetting("allow_experimental_inverted_index", 1);
        query_context->setSetting("allow_experimental_codecs", 1);
        query_context->setSetting("allow_experimental_live_view", 1);
        query_context->setSetting("allow_experimental_window_view", 1);
        query_context->setSetting("allow_experimental_funnel_functions", 1);
        query_context->setSetting("allow_experimental_nlp_functions", 1);
        query_context->setSetting("allow_experimental_hash_functions", 1);
        query_context->setSetting("allow_experimental_object_type", 1);
        query_context->setSetting("allow_experimental_annoy_index", 1);
        query_context->setSetting("allow_experimental_usearch_index", 1);
        query_context->setSetting("allow_experimental_bigint_types", 1);
        query_context->setSetting("allow_experimental_window_functions", 1);
        query_context->setSetting("allow_experimental_geo_types", 1);
        query_context->setSetting("allow_experimental_map_type", 1);

        query_context->setSetting("allow_suspicious_low_cardinality_types", 1);
        query_context->setSetting("allow_suspicious_fixed_string_types", 1);
        query_context->setSetting("allow_suspicious_indices", 1);
        query_context->setSetting("allow_suspicious_codecs", 1);
        query_context->setSetting("allow_hyperscan", 1);
        query_context->setSetting("allow_simdjson", 1);
        query_context->setSetting("allow_deprecated_syntax_for_merge_tree", 1);

        auto txn = std::make_shared<ZooKeeperMetadataTransaction>(current_zookeeper, zookeeper_path, false, "");
        query_context->initZooKeeperMetadataTransaction(txn);
        return query_context;
    };

    String db_name = getDatabaseName();
    String to_db_name = getDatabaseName() + BROKEN_TABLES_SUFFIX;
    String to_db_name_replicated = getDatabaseName() + BROKEN_REPLICATED_TABLES_SUFFIX;
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
        query_context->setSetting("allow_deprecated_database_ordinary", 1);
        executeQuery(query, query_context, true);

        /// But we want to avoid discarding UUID of ReplicatedMergeTree tables, because it will not work
        /// if zookeeper_path contains {uuid} macro. Replicated database do not recreate replicated tables on recovery,
        /// so it's ok to save UUID of replicated table.
        query = fmt::format("CREATE DATABASE IF NOT EXISTS {} ENGINE=Atomic", backQuoteIfNeed(to_db_name_replicated));
        query_context = Context::createCopy(getContext());
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

        auto move_table_to_database = [&](const String & broken_table_name, const String & to_database_name)
        {
            /// Table probably stores some data. Let's move it to another database.
            String to_name = fmt::format("{}_{}_{}", broken_table_name, max_log_ptr, thread_local_rng() % 1000);
            LOG_DEBUG(log, "Will RENAME TABLE {} TO {}.{}", backQuoteIfNeed(broken_table_name), backQuoteIfNeed(to_database_name), backQuoteIfNeed(to_name));
            assert(db_name < to_database_name);
            DDLGuardPtr to_table_guard = DatabaseCatalog::instance().getDDLGuard(to_database_name, to_name);
            auto to_db_ptr = DatabaseCatalog::instance().getDatabase(to_database_name);

            std::lock_guard lock{metadata_mutex};
            UInt64 new_digest = tables_metadata_digest;
            new_digest -= getMetadataHash(broken_table_name);
            DatabaseAtomic::renameTable(make_query_context(), broken_table_name, *to_db_ptr, to_name, /* exchange */ false, /* dictionary */ false);
            tables_metadata_digest = new_digest;
            assert(checkDigestValid(getContext()));
            ++moved_tables;
        };

        if (!table->storesDataOnDisk())
        {
            LOG_DEBUG(log, "Will DROP TABLE {}, because it does not store data on disk and can be safely dropped", backQuoteIfNeed(table_name));
            dropped_tables.push_back(tryGetTableUUID(table_name));
            dropped_dictionaries += table->isDictionary();
            table->flushAndShutdown();

            if (table->getName() == "MaterializedView" || table->getName() == "WindowView")
            {
                /// We have to drop MV inner table, so MV will not try to do it implicitly breaking some invariants.
                /// Also we have to commit metadata transaction, because it's not committed by default for inner tables of MVs.
                /// Yep, I hate inner tables of materialized views.
                auto mv_drop_inner_table_context = make_query_context();
                table->dropInnerTableIfAny(/* sync */ true, mv_drop_inner_table_context);
                mv_drop_inner_table_context->getZooKeeperMetadataTransaction()->commit();
            }

            std::lock_guard lock{metadata_mutex};
            UInt64 new_digest = tables_metadata_digest;
            new_digest -= getMetadataHash(table_name);
            DatabaseAtomic::dropTableImpl(make_query_context(), table_name, /* sync */ true);
            tables_metadata_digest = new_digest;
            assert(checkDigestValid(getContext()));
        }
        else if (!table->supportsReplication())
        {
            move_table_to_database(table_name, to_db_name);
        }
        else
        {
            move_table_to_database(table_name, to_db_name_replicated);
        }
    }

    if (!tables_to_detach.empty())
        LOG_WARNING(log, "Cleaned {} outdated objects: dropped {} dictionaries and {} tables, moved {} tables",
                    tables_to_detach.size(), dropped_dictionaries, dropped_tables.size() - dropped_dictionaries, moved_tables);

    /// Now database is cleared from outdated tables, let's rename ReplicatedMergeTree tables to actual names
    /// We have to take into account that tables names could be changed with two general queries
    /// 1) RENAME TABLE. There could be multiple pairs of tables (e.g. RENAME b TO c, a TO b, c TO d)
    /// But it is equal to multiple subsequent RENAMEs each of which operates only with two tables
    /// 2) EXCHANGE TABLE. This query swaps two names atomically and could not be represented with two separate RENAMEs
    auto rename_table = [&](String from, String to)
    {
        LOG_DEBUG(log, "Will RENAME TABLE {} TO {}", backQuoteIfNeed(from), backQuoteIfNeed(to));
        DDLGuardPtr table_guard = DatabaseCatalog::instance().getDDLGuard(db_name, std::min(from, to));
        DDLGuardPtr to_table_guard = DatabaseCatalog::instance().getDDLGuard(db_name, std::max(from, to));

        std::lock_guard lock{metadata_mutex};
        UInt64 new_digest = tables_metadata_digest;
        String statement = readMetadataFile(from);
        new_digest -= DB::getMetadataHash(from, statement);
        new_digest += DB::getMetadataHash(to, statement);
        DatabaseAtomic::renameTable(make_query_context(), from, *this, to, false, false);
        tables_metadata_digest = new_digest;
        assert(checkDigestValid(getContext()));
    };

    LOG_DEBUG(log, "Starting first stage of renaming process. Will rename tables to intermediate names");
    for (auto & [from, intermediate, _] : replicated_tables_to_rename)
    {
        /// Due to some unknown failures there could be tables
        /// which are already in an intermediate state
        /// For them we skip the first stage
        if (from == intermediate)
            continue;
        rename_table(from, intermediate);
    }
    LOG_DEBUG(log, "Starting second stage of renaming process. Will rename tables from intermediate to desired names");
    for (auto & [_, intermediate, to] : replicated_tables_to_rename)
        rename_table(intermediate, to);

    LOG_DEBUG(log, "Renames completed succesessfully");

    for (const auto & id : dropped_tables)
        DatabaseCatalog::instance().waitTableFinallyDropped(id);


    /// Create all needed tables in a proper order
    TablesDependencyGraph tables_dependencies("DatabaseReplicated (" + getDatabaseName() + ")");
    for (const auto & [table_name, create_table_query] : table_name_to_metadata)
    {
        /// Note that table_name could contain a dot inside (e.g. .inner.1234-1234-1234-1234)
        /// And QualifiedTableName::parseFromString doesn't handle this.
        auto qualified_name = QualifiedTableName{.database = getDatabaseName(), .table = table_name};
        auto query_ast = parseQueryFromMetadataInZooKeeper(table_name, create_table_query);
        tables_dependencies.addDependencies(qualified_name, getDependenciesFromCreateQuery(getContext(), qualified_name, query_ast));
    }

    tables_dependencies.checkNoCyclicDependencies();
    auto tables_to_create = tables_dependencies.getTablesSortedByDependency();

    for (const auto & table_id : tables_to_create)
    {
        auto table_name = table_id.getTableName();
        auto metadata_it = table_name_to_metadata.find(table_name);
        if (metadata_it == table_name_to_metadata.end())
        {
            /// getTablesSortedByDependency() may return some not existing tables or tables from other databases
            LOG_WARNING(log, "Got table name {} when resolving table dependencies, "
                        "but database {} does not have metadata for that table. Ignoring it", table_id.getNameForLogs(), getDatabaseName());
            continue;
        }

        const auto & create_query_string = metadata_it->second;
        if (isTableExist(table_name, getContext()))
        {
            assert(create_query_string == readMetadataFile(table_name) || getTableUUIDIfReplicated(create_query_string, getContext()) != UUIDHelpers::Nil);
            continue;
        }

        auto query_ast = parseQueryFromMetadataInZooKeeper(table_name, create_query_string);
        LOG_INFO(log, "Executing {}", serializeAST(*query_ast));
        auto create_query_context = make_query_context();
        InterpreterCreateQuery(query_ast, create_query_context).execute();
    }
    LOG_INFO(log, "All tables are created successfully");

    if (max_log_ptr_at_creation != 0)
    {
        /// If the replica is new and some of the queries applied during recovery
        /// where issued after the replica was created, then other nodes might be
        /// waiting for this node to notify them that the query was applied.
        for (UInt32 ptr = max_log_ptr_at_creation; ptr <= max_log_ptr; ++ptr)
        {
            auto entry_name = DDLTaskBase::getLogEntryName(ptr);
            auto path = fs::path(zookeeper_path) / "log" / entry_name / "finished" / getFullReplicaName();
            auto status = ExecutionStatus(0).serializeText();
            auto res = current_zookeeper->tryCreate(path, status, zkutil::CreateMode::Persistent);
            if (res == Coordination::Error::ZOK)
                LOG_INFO(log, "Marked recovered {} as finished", entry_name);
        }
    }

    std::lock_guard lock{metadata_mutex};
    chassert(checkDigestValid(getContext()));
    current_zookeeper->set(replica_path + "/digest", toString(tables_metadata_digest));
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
            chassert(max_log_ptr == new_max_log_ptr);
            chassert(table_names.size() != table_name_to_metadata.size());
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
    if (create.uuid == UUIDHelpers::Nil || create.getTable() != TABLE_WITH_UUID_NAME_PLACEHOLDER || create.database)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected query from {}: {}", node_name, query);

    bool is_materialized_view_with_inner_table = create.is_materialized_view && create.to_table_id.empty();

    create.setDatabase(getDatabaseName());
    create.setTable(unescapeForFileName(node_name));
    create.attach = is_materialized_view_with_inner_table;

    return ast;
}

void DatabaseReplicated::dropReplica(
    DatabaseReplicated * database, const String & database_zookeeper_path, const String & shard, const String & replica)
{
    assert(!database || database_zookeeper_path == database->zookeeper_path);

    String full_replica_name = shard.empty() ? replica : getFullReplicaName(shard, replica);

    if (full_replica_name.find('/') != std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid replica name, '/' is not allowed: {}", full_replica_name);

    auto zookeeper = Context::getGlobalContextInstance()->getZooKeeper();

    String database_mark = zookeeper->get(database_zookeeper_path);
    if (database_mark != REPLICATED_DATABASE_MARK)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} does not look like a path of Replicated database", database_zookeeper_path);

    String database_replica_path = fs::path(database_zookeeper_path) / "replicas" / full_replica_name;
    if (!zookeeper->exists(database_replica_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica {} does not exist (database path: {})",
                        full_replica_name, database_zookeeper_path);

    if (zookeeper->exists(database_replica_path + "/active"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica {} is active, cannot drop it (database path: {})",
                        full_replica_name, database_zookeeper_path);

    zookeeper->set(database_replica_path, DROPPED_MARK, -1);
    /// Notify other replicas that cluster configuration was changed (if we can)
    if (database)
        database->createEmptyLogEntry(zookeeper);

    zookeeper->tryRemoveRecursive(database_replica_path);
    if (zookeeper->tryRemove(database_zookeeper_path + "/replicas") == Coordination::Error::ZOK)
    {
        /// It was the last replica, remove all metadata
        zookeeper->tryRemoveRecursive(database_zookeeper_path);
    }
}

void DatabaseReplicated::drop(ContextPtr context_)
{
    if (is_probably_dropped)
    {
        /// Don't need to drop anything from ZooKeeper
        DatabaseAtomic::drop(context_);
        return;
    }

    auto current_zookeeper = getZooKeeper();
    current_zookeeper->set(replica_path, DROPPED_MARK, -1);
    createEmptyLogEntry(current_zookeeper);

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
    ddl_worker_initialized = false;
    ddl_worker = nullptr;
    DatabaseAtomic::shutdown();
}


void DatabaseReplicated::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker || !ddl_worker->isCurrentlyActive() || txn || startsWith(table_name, ".inner_id."));
    if (txn && txn->isInitialQuery() && !txn->isCreateOrReplaceQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));
    }

    auto table = tryGetTable(table_name, getContext());
    if (table->getName() == "MaterializedView" || table->getName() == "WindowView")
    {
        /// Avoid recursive locking of metadata_mutex
        table->dropInnerTableIfAny(sync, local_context);
    }

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    new_digest -= getMetadataHash(table_name);
    if (txn && !txn->isCreateOrReplaceQuery() && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::dropTableImpl(local_context, table_name, sync);
    tables_metadata_digest = new_digest;

    assert(checkDigestValid(local_context));
}

void DatabaseReplicated::renameTable(ContextPtr local_context, const String & table_name, IDatabase & to_database,
                                     const String & to_table_name, bool exchange, bool dictionary)
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(txn);

    if (this != &to_database)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases is not supported for Replicated engine");
    if (table_name == to_table_name)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot rename table to itself");
    if (!isTableExist(table_name, local_context))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", table_name);
    if (exchange && !to_database.isTableExist(to_table_name, local_context))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", to_table_name);

    String statement = readMetadataFile(table_name);
    String statement_to;
    if (exchange)
        statement_to = readMetadataFile(to_table_name);

    if (txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        String metadata_zk_path_to = zookeeper_path + "/metadata/" + escapeForFileName(to_table_name);
        if (!txn->isCreateOrReplaceQuery())
            txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));

        if (exchange)
        {
            txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path_to, -1));
            if (!txn->isCreateOrReplaceQuery())
                txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, statement_to, zkutil::CreateMode::Persistent));
        }
        txn->addOp(zkutil::makeCreateRequest(metadata_zk_path_to, statement, zkutil::CreateMode::Persistent));
    }

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    new_digest -= DB::getMetadataHash(table_name, statement);
    new_digest += DB::getMetadataHash(to_table_name, statement);
    if (exchange)
    {
        new_digest -= DB::getMetadataHash(to_table_name, statement_to);
        new_digest += DB::getMetadataHash(table_name, statement_to);
    }
    if (txn && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
    tables_metadata_digest = new_digest;
    assert(checkDigestValid(local_context));
}

void DatabaseReplicated::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                       ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);

    String statement = getObjectDefinitionFromCreateQuery(query.clone());
    if (txn && txn->isInitialQuery() && !txn->isCreateOrReplaceQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(query.getTable());
        /// zk::multi(...) will throw if `metadata_zk_path` exists
        txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, statement, zkutil::CreateMode::Persistent));
    }

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    new_digest += DB::getMetadataHash(query.getTable(), statement);
    if (txn && !txn->isCreateOrReplaceQuery() && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::commitCreateTable(query, table, table_metadata_tmp_path, table_metadata_path, query_context);
    tables_metadata_digest = new_digest;
    assert(checkDigestValid(query_context));
}

void DatabaseReplicated::commitAlterTable(const StorageID & table_id,
                                          const String & table_metadata_tmp_path, const String & table_metadata_path,
                                          const String & statement, ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker || !ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_id.table_name);
        txn->addOp(zkutil::makeSetRequest(metadata_zk_path, statement, -1));
    }

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    new_digest -= getMetadataHash(table_id.table_name);
    new_digest += DB::getMetadataHash(table_id.table_name, statement);
    if (txn && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path, statement, query_context);
    tables_metadata_digest = new_digest;
    assert(checkDigestValid(query_context));
}


bool DatabaseReplicated::canExecuteReplicatedMetadataAlter() const
{
    /// ReplicatedMergeTree may call commitAlterTable from its background threads when executing ALTER_METADATA entries.
    /// It may update the metadata digest (both locally and in ZooKeeper)
    /// before DatabaseReplicatedDDLWorker::initializeReplication() has finished.
    /// We should not update metadata until the database is initialized.
    return ddl_worker_initialized && ddl_worker->isCurrentlyActive();
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

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    new_digest -= getMetadataHash(table_name);
    if (txn && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::detachTablePermanently(local_context, table_name);
    tables_metadata_digest = new_digest;
    assert(checkDigestValid(local_context));
}

void DatabaseReplicated::removeDetachedPermanentlyFlag(ContextPtr local_context, const String & table_name, const String & table_metadata_path, bool attach)
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);
    if (txn && txn->isInitialQuery() && attach)
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        String statement = readMetadataFile(table_name);
        txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, statement, zkutil::CreateMode::Persistent));
    }

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    if (attach)
    {
        new_digest += getMetadataHash(table_name);
        if (txn && !is_recovering)
            txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));
    }

    DatabaseAtomic::removeDetachedPermanentlyFlag(local_context, table_name, table_metadata_path, attach);
    tables_metadata_digest = new_digest;
    assert(checkDigestValid(local_context));
}


String DatabaseReplicated::readMetadataFile(const String & table_name) const
{
    String statement;
    ReadBufferFromFile in(getObjectMetadataPath(table_name), METADATA_FILE_BUFFER_SIZE);
    readStringUntilEOF(statement, in);
    return statement;
}


std::vector<std::pair<ASTPtr, StoragePtr>>
DatabaseReplicated::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr &) const
{
    /// Here we read metadata from ZooKeeper. We could do that by simple call of DatabaseAtomic::getTablesForBackup() however
    /// reading from ZooKeeper is better because thus we won't be dependent on how fast the replication queue of this database is.
    std::vector<std::pair<ASTPtr, StoragePtr>> res;
    auto zookeeper = getContext()->getZooKeeper();
    auto escaped_table_names = zookeeper->getChildren(zookeeper_path + "/metadata");
    for (const auto & escaped_table_name : escaped_table_names)
    {
        String table_name = unescapeForFileName(escaped_table_name);
        if (!filter(table_name))
            continue;

        String zk_metadata;
        if (!zookeeper->tryGet(zookeeper_path + "/metadata/" + escaped_table_name, zk_metadata))
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP, "Metadata for table {} was not found in ZooKeeper", table_name);

        ParserCreateQuery parser;
        auto create_table_query = parseQuery(parser, zk_metadata, 0, getContext()->getSettingsRef().max_parser_depth);

        auto & create = create_table_query->as<ASTCreateQuery &>();
        create.attach = false;
        create.setTable(table_name);
        create.setDatabase(getDatabaseName());

        StoragePtr storage;
        if (create.uuid != UUIDHelpers::Nil)
        {
            storage = DatabaseCatalog::instance().tryGetByUUID(create.uuid).second;
            if (storage)
                storage->adjustCreateQueryForBackup(create_table_query);
        }

        /// `storage` is allowed to be null here. In this case it means that this storage exists on other replicas
        /// but it has not been created on this replica yet.

        res.emplace_back(create_table_query, storage);
    }

    return res;
}


void DatabaseReplicated::createTableRestoredFromBackup(
    const ASTPtr & create_table_query,
    ContextMutablePtr local_context,
    std::shared_ptr<IRestoreCoordination> restore_coordination,
    UInt64 timeout_ms)
{
    /// Because of the replication multiple nodes can try to restore the same tables again and failed with "Table already exists"
    /// because of some table could be restored already on other node and then replicated to this node.
    /// To solve this problem we use the restore coordination: the first node calls
    /// IRestoreCoordination::acquireCreatingTableInReplicatedDatabase() and then for other nodes this function returns false which means
    /// this table is already being created by some other node.
    String table_name = create_table_query->as<const ASTCreateQuery &>().getTable();
    if (restore_coordination->acquireCreatingTableInReplicatedDatabase(getZooKeeperPath(), table_name))
    {
        DatabaseAtomic::createTableRestoredFromBackup(create_table_query, local_context, restore_coordination, timeout_ms);
    }

    /// Wait until the table is actually created no matter if it's created by the current or another node and replicated to the
    /// current node afterwards. We have to wait because `RestorerFromBackup` is going to restore data of the table then.
    /// TODO: The following code doesn't look very reliable, probably we need to rewrite it somehow.
    auto timeout = std::chrono::milliseconds{timeout_ms};
    auto start_time = std::chrono::steady_clock::now();
    while (!isTableExist(table_name, local_context))
    {
        waitForReplicaToProcessAllEntries(50);

        auto elapsed = std::chrono::steady_clock::now() - start_time;
        if (elapsed > timeout)
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                            "Couldn't restore table {}.{} on other node or sync it (elapsed {})",
                            backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(table_name), to_string(elapsed));
    }
}

bool DatabaseReplicated::shouldReplicateQuery(const ContextPtr & query_context, const ASTPtr & query_ptr) const
{
    if (query_context->getClientInfo().is_replicated_database_internal)
        return false;

    /// we never replicate KeeperMap operations for some types of queries because it doesn't make sense
    const auto is_keeper_map_table = [&](const ASTPtr & ast)
    {
        auto table_id = query_context->resolveStorageID(ast, Context::ResolveOrdinary);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, query_context);

        return table->as<StorageKeeperMap>() != nullptr;
    };

    const auto is_replicated_table = [&](const ASTPtr & ast)
    {
        auto table_id = query_context->resolveStorageID(ast, Context::ResolveOrdinary);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, query_context);

        return table->supportsReplication();
    };

    const auto has_many_shards = [&]()
    {
        /// If there is only 1 shard then there is no need to replicate some queries.
        auto current_cluster = tryGetCluster();
        return
            !current_cluster || /// Couldn't get the cluster, so we don't know how many shards there are.
            current_cluster->getShardsInfo().size() > 1;
    };

    /// Some ALTERs are not replicated on database level
    if (const auto * alter = query_ptr->as<const ASTAlterQuery>())
    {
        if (alter->isAttachAlter() || alter->isFetchAlter() || alter->isDropPartitionAlter() || is_keeper_map_table(query_ptr) || alter->isFreezeAlter())
            return false;

        if (has_many_shards() || !is_replicated_table(query_ptr))
            return true;

        try
        {
            /// Metadata alter should go through database
            for (const auto & child : alter->command_list->children)
                if (AlterCommand::parse(child->as<ASTAlterCommand>()))
                    return true;

            /// It's ALTER PARTITION or mutation, doesn't involve database
            return false;
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

        return true;
    }

    /// DROP DATABASE is not replicated
    if (const auto * drop = query_ptr->as<const ASTDropQuery>())
    {
        if (drop->table.get())
            return drop->kind != ASTDropQuery::Truncate || !is_keeper_map_table(query_ptr);

        return false;
    }

    if (query_ptr->as<const ASTDeleteQuery>() != nullptr)
    {
        if (is_keeper_map_table(query_ptr))
            return false;

        return has_many_shards() || !is_replicated_table(query_ptr);
    }

    return true;
}

}
