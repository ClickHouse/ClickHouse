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
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Databases/DatabaseReplicatedWorker.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/Cluster.h>
#include <base/getFQDNOrHostName.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/formatAST.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Common/Macros.h>
#include <base/chrono_io.h>

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

    if (!db_settings.collection_name.value.empty())
        fillClusterAuthInfo(db_settings.collection_name.value, context_->getConfigRef());
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

    UInt16 default_port = getContext()->getTCPPort();

    bool treat_local_as_remote = false;
    bool treat_local_port_as_remote = getContext()->getApplicationType() == Context::ApplicationType::LOCAL;
    return std::make_shared<Cluster>(
        getContext()->getSettingsRef(),
        shards,
        cluster_auth_info.cluster_username,
        cluster_auth_info.cluster_password,
        default_port,
        treat_local_as_remote,
        treat_local_port_as_remote,
        cluster_auth_info.cluster_secure_connection,
        /*priority=*/1,
        TSA_SUPPRESS_WARNING_FOR_READ(database_name),     /// FIXME
        cluster_auth_info.cluster_secret);
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
            throw Exception("Can't create replicated database without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
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
                LOG_WARNING(log, "Database {} exists locally, but marked dropped in ZooKeeper {}. "
                                 "Will not try to start it up", getDatabaseName(), replica_path);
                is_probably_dropped = true;
                return;
            }

            String host_id = getHostID(getContext(), db_uuid);
            if (is_create_query || replica_host_id != host_id)
            {
                throw Exception(
                    ErrorCodes::REPLICA_IS_ALREADY_EXIST,
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
            LOG_WARNING(log, "Database {} exists locally, but its replica does not exist in ZooKeeper {}. "
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
    chassert(false);
    __builtin_unreachable();
}

bool DatabaseReplicated::looksLikeReplicatedDatabasePath(const ZooKeeperPtr & current_zookeeper, const String & path)
{
    Coordination::Stat stat;
    String maybe_database_mark;
    if (!current_zookeeper->tryGet(path, maybe_database_mark, &stat))
        return false;
    if (maybe_database_mark.starts_with(REPLICATED_DATABASE_MARK))
        return true;
    if (maybe_database_mark.empty())
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
    if (!ddl_worker)
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

void DatabaseReplicated::loadStoredObjects(
    ContextMutablePtr local_context, LoadingStrictnessLevel mode, bool skip_startup_tables)
{
    beforeLoadingMetadata(local_context, mode);
    DatabaseAtomic::loadStoredObjects(local_context, mode, skip_startup_tables);
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
    chassert(!tables_metadata_digest);
    tables_metadata_digest = digest;

    ddl_worker = std::make_unique<DatabaseReplicatedDDLWorker>(this, getContext());
    if (is_probably_dropped)
        return;
    ddl_worker->startup();
}

bool DatabaseReplicated::debugCheckDigest(const ContextPtr & local_context) const
{
    /// Reduce number of debug checks
    //if (thread_local_rng() % 16)
    //    return true;

    LOG_TEST(log, "Current in-memory metadata digest: {}", tables_metadata_digest);

    /// Database is probably being dropped
    if (!local_context->getZooKeeperMetadataTransaction() && !ddl_worker->isCurrentlyActive())
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
                LOG_WARNING(log, "It's not recommended to explicitly specify zookeeper_path and replica_name in ReplicatedMergeTree arguments");

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
    chassert(create.uuid != UUIDHelpers::Nil);
    return create.uuid;
}

void DatabaseReplicated::recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 max_log_ptr)
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
            /// Local table does not exist in ZooKeeper or has different metadata
            tables_to_detach.emplace_back(std::move(name));
        }
    }

    auto make_query_context = [this, current_zookeeper]()
    {
        auto query_context = Context::createCopy(getContext());
        query_context->makeQueryContext();
        query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        query_context->getClientInfo().is_replicated_database_internal = true;
        query_context->setCurrentDatabase(getDatabaseName());
        query_context->setCurrentQueryId("");
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
            assert(debugCheckDigest(getContext()));
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
                table->dropInnerTableIfAny(sync, mv_drop_inner_table_context);
                mv_drop_inner_table_context->getZooKeeperMetadataTransaction()->commit();
            }

            std::lock_guard lock{metadata_mutex};
            UInt64 new_digest = tables_metadata_digest;
            new_digest -= getMetadataHash(table_name);
            DatabaseAtomic::dropTableImpl(make_query_context(), table_name, /* sync */ true);
            tables_metadata_digest = new_digest;
            assert(debugCheckDigest(getContext()));
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
    for (const auto & old_to_new : replicated_tables_to_rename)
    {
        const String & from = old_to_new.first;
        const String & to = old_to_new.second;

        LOG_DEBUG(log, "Will RENAME TABLE {} TO {}", backQuoteIfNeed(from), backQuoteIfNeed(to));
        /// TODO Maybe we should do it in two steps: rename all tables to temporary names and then rename them to actual names?
        DDLGuardPtr table_guard = DatabaseCatalog::instance().getDDLGuard(db_name, std::min(from, to));
        DDLGuardPtr to_table_guard = DatabaseCatalog::instance().getDDLGuard(db_name, std::max(from, to));

        std::lock_guard lock{metadata_mutex};
        UInt64 new_digest = tables_metadata_digest;
        String statement = readMetadataFile(from);
        new_digest -= DB::getMetadataHash(from, statement);
        new_digest += DB::getMetadataHash(to, statement);
        DatabaseAtomic::renameTable(make_query_context(), from, *this, to, false, false);
        tables_metadata_digest = new_digest;
        assert(debugCheckDigest(getContext()));
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
        LOG_INFO(log, "Executing {}", serializeAST(*query_ast));
        auto create_query_context = make_query_context();
        InterpreterCreateQuery(query_ast, create_query_context).execute();
    }

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
    chassert(debugCheckDigest(getContext()));
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

void DatabaseReplicated::drop(ContextPtr context_)
{
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
    ddl_worker = nullptr;
    DatabaseAtomic::shutdown();
}


void DatabaseReplicated::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn || startsWith(table_name, ".inner_id."));
    if (txn && txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));
    }

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    new_digest -= getMetadataHash(table_name);
    if (txn)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::dropTable(local_context, table_name, sync);
    tables_metadata_digest = new_digest;

    assert(debugCheckDigest(local_context));
}

void DatabaseReplicated::renameTable(ContextPtr local_context, const String & table_name, IDatabase & to_database,
                                     const String & to_table_name, bool exchange, bool dictionary)
{
    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(txn);

    String statement = readMetadataFile(table_name);
    String statement_to;
    if (exchange)
        statement_to = readMetadataFile(to_table_name);

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

        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        String metadata_zk_path_to = zookeeper_path + "/metadata/" + escapeForFileName(to_table_name);
        txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));
        if (exchange)
        {
            txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path_to, -1));
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
    if (txn)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
    tables_metadata_digest = new_digest;
    assert(debugCheckDigest(local_context));
}

void DatabaseReplicated::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                       ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker->isCurrentlyActive() || txn);

    String statement = getObjectDefinitionFromCreateQuery(query.clone());
    if (txn && txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(query.getTable());
        /// zk::multi(...) will throw if `metadata_zk_path` exists
        txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, statement, zkutil::CreateMode::Persistent));
    }

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    new_digest += DB::getMetadataHash(query.getTable(), statement);
    if (txn)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::commitCreateTable(query, table, table_metadata_tmp_path, table_metadata_path, query_context);
    tables_metadata_digest = new_digest;
    assert(debugCheckDigest(query_context));
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

    std::lock_guard lock{metadata_mutex};
    UInt64 new_digest = tables_metadata_digest;
    new_digest -= getMetadataHash(table_id.table_name);
    new_digest += DB::getMetadataHash(table_id.table_name, statement);
    if (txn)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::commitAlterTable(table_id, table_metadata_tmp_path, table_metadata_path, statement, query_context);
    tables_metadata_digest = new_digest;
    assert(debugCheckDigest(query_context));
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
    if (txn)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    DatabaseAtomic::detachTablePermanently(local_context, table_name);
    tables_metadata_digest = new_digest;
    assert(debugCheckDigest(local_context));
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
        if (txn)
            txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));
    }

    DatabaseAtomic::removeDetachedPermanentlyFlag(local_context, table_name, table_metadata_path, attach);
    tables_metadata_digest = new_digest;
    assert(debugCheckDigest(local_context));
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

}
