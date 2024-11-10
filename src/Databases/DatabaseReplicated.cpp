#include <DataTypes/DataTypeString.h>

#include <utility>

#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/DatabaseReplicatedWorker.h>
#include <Databases/TablesDependencyGraph.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ReplicatedDatabaseQueryStatusSource.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Sinks/EmptySink.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageKeeperMap.h>
#include <base/chrono_io.h>
#include <base/getFQDNOrHostName.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/PoolId.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 database_replicated_allow_replicated_engine_arguments;
    extern const SettingsBool database_replicated_always_detach_permanently;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsDistributedDDLOutputMode distributed_ddl_output_mode;
    extern const SettingsInt64 distributed_ddl_task_timeout;
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
}

namespace ServerSetting
{
    extern const ServerSettingsBool database_replicated_allow_detach_permanently;
    extern const ServerSettingsUInt32 max_database_replicated_create_table_thread_pool_size;
}

namespace DatabaseReplicatedSetting
{
    extern const DatabaseReplicatedSettingsString collection_name;
    extern const DatabaseReplicatedSettingsFloat max_broken_tables_ratio;
}

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
    extern const int CANNOT_GET_REPLICATED_DATABASE_SNAPSHOT;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int QUERY_IS_PROHIBITED;
    extern const int SUPPORT_IS_DISABLED;
}

static constexpr const char * REPLICATED_DATABASE_MARK = "DatabaseReplicated";
static constexpr const char * DROPPED_MARK = "DROPPED";
static constexpr const char * BROKEN_TABLES_SUFFIX = "_broken_tables";
static constexpr const char * BROKEN_REPLICATED_TABLES_SUFFIX = "_broken_replicated_tables";
static constexpr const char * FIRST_REPLICA_DATABASE_NAME = "first_replica_database_name";

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

zkutil::ZooKeeperPtr DatabaseReplicated::getZooKeeper() const
{
    return getContext()->getZooKeeper();
}

static inline String getHostID(ContextPtr global_context, const UUID & db_uuid, bool secure)
{
    UInt16 port = secure ? global_context->getTCPPortSecure().value_or(DBMS_DEFAULT_SECURE_PORT) : global_context->getTCPPort();
    return Cluster::Address::toString(getFQDNOrHostName(), port) + ':' + toString(db_uuid);
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

    if (!db_settings[DatabaseReplicatedSetting::collection_name].value.empty())
        fillClusterAuthInfo(db_settings[DatabaseReplicatedSetting::collection_name].value, context_->getConfigRef());

    replica_group_name = context_->getConfigRef().getString("replica_group_name", "");

    if (!replica_group_name.empty() && database_name.starts_with(DatabaseReplicated::ALL_GROUPS_CLUSTER_PREFIX))
    {
        context_->addWarningMessage(fmt::format("There's a Replicated database with a name starting from '{}', "
                                                "and replica_group_name is configured. It may cause collisions in cluster names.",
                                                ALL_GROUPS_CLUSTER_PREFIX));
    }
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

ClusterPtr DatabaseReplicated::tryGetAllGroupsCluster() const
{
    std::lock_guard lock{mutex};
    if (replica_group_name.empty())
        return nullptr;

    if (cluster_all_groups)
        return cluster_all_groups;

    /// Database is probably not created or not initialized yet, it's ok to return nullptr
    if (is_readonly)
        return cluster_all_groups;

    try
    {
        cluster_all_groups = getClusterImpl(/*all_groups*/ true);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
    return cluster_all_groups;
}

void DatabaseReplicated::setCluster(ClusterPtr && new_cluster, bool all_groups)
{
    std::lock_guard lock{mutex};
    if (all_groups)
        cluster_all_groups = std::move(new_cluster);
    else
        cluster = std::move(new_cluster);
}

ClusterPtr DatabaseReplicated::getClusterImpl(bool all_groups) const
{
    Strings unfiltered_hosts;
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
        unfiltered_hosts = zookeeper->getChildren(zookeeper_path + "/replicas", &stat);
        if (unfiltered_hosts.empty())
            throw Exception(ErrorCodes::NO_ACTIVE_REPLICAS, "No replicas of database {} found. "
                            "It's possible if the first replica is not fully created yet "
                            "or if the last replica was just dropped or due to logical error", zookeeper_path);

        if (all_groups)
        {
            hosts = unfiltered_hosts;
        }
        else
        {
            hosts.clear();
            std::vector<String> paths;
            for (const auto & host : unfiltered_hosts)
                paths.push_back(zookeeper_path + "/replicas/" + host + "/replica_group");

            auto replica_groups = zookeeper->tryGet(paths);

            for (size_t i = 0; i < paths.size(); ++i)
            {
                if (replica_groups[i].data == replica_group_name)
                    hosts.push_back(unfiltered_hosts[i]);
            }
        }

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

    LOG_TRACE(log, "Got a list of hosts after {} iterations. All hosts: [{}], filtered: [{}], ids: [{}]", iteration,
              fmt::join(unfiltered_hosts, ", "), fmt::join(hosts, ", "), fmt::join(host_ids, ", "));

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

    UInt16 default_port;
    if (cluster_auth_info.cluster_secure_connection)
        default_port = getContext()->getTCPPortSecure().value_or(DBMS_DEFAULT_SECURE_PORT);
    else
        default_port = getContext()->getTCPPort();

    bool treat_local_as_remote = false;
    bool treat_local_port_as_remote = getContext()->getApplicationType() == Context::ApplicationType::LOCAL;

    String cluster_name = TSA_SUPPRESS_WARNING_FOR_READ(database_name);     /// FIXME
    if (all_groups)
        cluster_name = ALL_GROUPS_CLUSTER_PREFIX + cluster_name;

    ClusterConnectionParameters params{
        cluster_auth_info.cluster_username,
        cluster_auth_info.cluster_password,
        default_port,
        treat_local_as_remote,
        treat_local_port_as_remote,
        cluster_auth_info.cluster_secure_connection,
        Priority{1},
        cluster_name,
        cluster_auth_info.cluster_secret};

    return std::make_shared<Cluster>(getContext()->getSettingsRef(), shards, params);
}

ReplicasInfo DatabaseReplicated::tryGetReplicasInfo(const ClusterPtr & cluster_) const
{
    Strings paths;

    paths.emplace_back(fs::path(zookeeper_path) / "max_log_ptr");

    const auto & addresses_with_failover = cluster_->getShardsAddresses();
    const auto & shards_info = cluster_->getShardsInfo();
    for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
    {
        for (const auto & replica : addresses_with_failover[shard_index])
        {
            String full_name = getFullReplicaName(replica.database_shard_name, replica.database_replica_name);
            paths.emplace_back(fs::path(zookeeper_path) / "replicas" / full_name / "active");
            paths.emplace_back(fs::path(zookeeper_path) / "replicas" / full_name / "log_ptr");
        }
    }

    try
    {
        auto current_zookeeper = getZooKeeper();
        auto zk_res = current_zookeeper->tryGet(paths);

        auto max_log_ptr_zk = zk_res[0];
        if (max_log_ptr_zk.error != Coordination::Error::ZOK)
            throw Coordination::Exception(max_log_ptr_zk.error);

        UInt32 max_log_ptr = parse<UInt32>(max_log_ptr_zk.data);

        ReplicasInfo replicas_info;
        replicas_info.resize((zk_res.size() - 1) / 2);

        size_t global_replica_index = 0;
        for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
        {
            for (const auto & replica : addresses_with_failover[shard_index])
            {
                auto replica_active = zk_res[2 * global_replica_index + 1];
                auto replica_log_ptr = zk_res[2 * global_replica_index + 2];

                UInt64 recovery_time = 0;
                {
                    std::lock_guard lock(ddl_worker_mutex);
                    if (replica.is_local && ddl_worker)
                        recovery_time = ddl_worker->getCurrentInitializationDurationMs();
                }

                replicas_info[global_replica_index] = ReplicaInfo{
                    .is_active = replica_active.error == Coordination::Error::ZOK,
                    .replication_lag = replica_log_ptr.error != Coordination::Error::ZNONODE ? std::optional(max_log_ptr - parse<UInt32>(replica_log_ptr.data)) : std::nullopt,
                    .recovery_time = recovery_time,
                };

                ++global_replica_index;
            }
        }

        return replicas_info;
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
        bool replica_exists_in_zk = current_zookeeper->tryGet(replica_path, replica_host_id);
        if (replica_exists_in_zk)
        {
            if (replica_host_id == DROPPED_MARK && !is_create_query)
            {
                LOG_WARNING(log, "Database {} exists locally, but marked dropped in ZooKeeper ({}). "
                                 "Will not try to start it up", getDatabaseName(), replica_path);
                is_probably_dropped = true;
                return;
            }

            String host_id = getHostID(getContext(), db_uuid, cluster_auth_info.cluster_secure_connection);
            String host_id_default = getHostID(getContext(), db_uuid, false);

            if (replica_host_id != host_id && replica_host_id != host_id_default)
            {
                throw Exception(
                    ErrorCodes::REPLICA_ALREADY_EXISTS,
                    "Replica {} of shard {} of replicated database at {} already exists. Replica host ID: '{}', current host ID: '{}'",
                    replica_name, shard_name, zookeeper_path, replica_host_id, host_id);
            }

            /// Before 24.6 we always created host_id with insecure port, even if cluster_auth_info.cluster_secure_connection was true.
            /// So not to break compatibility, we need to update host_id to secure one if cluster_auth_info.cluster_secure_connection is true.
            if (host_id != host_id_default && replica_host_id == host_id_default)
            {
                current_zookeeper->set(replica_path, host_id, -1);
                createEmptyLogEntry(current_zookeeper);
            }

            /// Check that replica_group_name in ZooKeeper matches the local one and change it if necessary.
            String zk_replica_group_name;
            if (!current_zookeeper->tryGet(replica_path + "/replica_group", zk_replica_group_name))
            {
                /// Replica groups were introduced in 23.10, so the node might not exist
                current_zookeeper->create(replica_path + "/replica_group", replica_group_name, zkutil::CreateMode::Persistent);
                if (!replica_group_name.empty())
                    createEmptyLogEntry(current_zookeeper);
            }
            else if (zk_replica_group_name != replica_group_name)
            {
                current_zookeeper->set(replica_path + "/replica_group", replica_group_name, -1);
                createEmptyLogEntry(current_zookeeper);
            }

            /// Needed to mark all the queries
            /// in the range (max log ptr at replica ZooKeeper nodes creation, max log ptr after replica recovery] as successful.
            String max_log_ptr_at_creation_str;
            if (current_zookeeper->tryGet(replica_path + "/max_log_ptr_at_creation", max_log_ptr_at_creation_str))
                max_log_ptr_at_creation = parse<UInt32>(max_log_ptr_at_creation_str);
        }

        if (is_create_query)
        {
            /// Create replica nodes in ZooKeeper. If newly initialized nodes already exist, reuse them.
            createReplicaNodesInZooKeeper(current_zookeeper);
        }
        else if (!replica_exists_in_zk)
        {
            /// It's not CREATE query, but replica does not exist. Probably it was dropped.
            /// Do not create anything, continue as readonly.
            LOG_WARNING(log, "Database {} exists locally, but its replica does not exist in ZooKeeper ({}). "
                             "Assuming it was dropped, will not try to start it up", getDatabaseName(), replica_path);
            is_probably_dropped = true;
            return;
        }

        /// If not exist, create a node with the database name for introspection.
        /// Technically, the database may have different names on different replicas, but this is not a usual case and we only save the first one
        auto db_name_path = fs::path(zookeeper_path) / FIRST_REPLICA_DATABASE_NAME;
        auto error_code = current_zookeeper->trySet(db_name_path, getDatabaseName());
        if (error_code == Coordination::Error::ZNONODE)
            current_zookeeper->tryCreate(db_name_path, getDatabaseName(), zkutil::CreateMode::Persistent);

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
    {
        std::lock_guard lock{ddl_worker_mutex};
        if (!ddl_worker || is_probably_dropped)
            return false;
    }
    return ddl_worker->waitForReplicaToProcessAllEntries(timeout_ms);
}

void DatabaseReplicated::createReplicaNodesInZooKeeper(const zkutil::ZooKeeperPtr & current_zookeeper)
{
    if (!looksLikeReplicatedDatabasePath(current_zookeeper, zookeeper_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot add new database replica: provided path {} "
                        "already contains some data and it does not look like Replicated database path.", zookeeper_path);

    /// Write host name to replica_path, it will protect from multiple replicas with the same name
    const auto host_id = getHostID(getContext(), db_uuid, cluster_auth_info.cluster_secure_connection);

    const std::vector<String> check_paths = {
        replica_path,
        replica_path + "/replica_group",
        replica_path + "/digest",
    };
    bool nodes_exist = true;
    auto check_responses = current_zookeeper->tryGet(check_paths);
    for (size_t i = 0; i < check_responses.size(); ++i)
    {
        const auto response = check_responses[i];

        if (response.error == Coordination::Error::ZNONODE)
        {
            nodes_exist = false;
            break;
        }
        if (response.error != Coordination::Error::ZOK)
        {
            throw zkutil::KeeperException::fromPath(response.error, check_paths[i]);
        }
    }

    if (nodes_exist)
    {
        const std::vector<String> expected_data = {
            host_id,
            replica_group_name,
            "0",
        };
        for (size_t i = 0; i != expected_data.size(); ++i)
        {
            if (check_responses[i].data != expected_data[i])
            {
                throw Exception(
                    ErrorCodes::REPLICA_ALREADY_EXISTS,
                    "Replica node {} in ZooKeeper already exists and contains unexpected value: {}",
                    quoteString(check_paths[i]), quoteString(check_responses[i].data));
            }
        }

        LOG_DEBUG(log, "Newly initialized replica nodes found in ZooKeeper, reusing them");
        createEmptyLogEntry(current_zookeeper);
        return;
    }

    for (int attempts = 10; attempts > 0; --attempts)
    {
        Coordination::Stat stat;
        const String max_log_ptr_str = current_zookeeper->get(zookeeper_path + "/max_log_ptr", &stat);

        const Coordination::Requests ops = {
            zkutil::makeCreateRequest(replica_path, host_id, zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(replica_path + "/log_ptr", "0", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(replica_path + "/digest", "0", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(replica_path + "/replica_group", replica_group_name, zkutil::CreateMode::Persistent),

            /// Previously, this method was not idempotent and max_log_ptr_at_creation could be stored in memory.
            /// we need to store max_log_ptr_at_creation in ZooKeeper to make this method idempotent during replica creation.
            zkutil::makeCreateRequest(replica_path + "/max_log_ptr_at_creation", max_log_ptr_str, zkutil::CreateMode::Persistent),
            zkutil::makeCheckRequest(zookeeper_path + "/max_log_ptr", stat.version),
        };

        Coordination::Responses ops_responses;
        const auto code = current_zookeeper->tryMulti(ops, ops_responses);

        if (code == Coordination::Error::ZOK)
        {
            max_log_ptr_at_creation = parse<UInt32>(max_log_ptr_str);
            createEmptyLogEntry(current_zookeeper);
            return;
        }

        if (attempts == 1)
        {
            zkutil::KeeperMultiException::check(code, ops, ops_responses);
        }
    }
}

void DatabaseReplicated::beforeLoadingMetadata(ContextMutablePtr context_, LoadingStrictnessLevel mode)
{
    DatabaseAtomic::beforeLoadingMetadata(context_, mode);
    tryConnectToZooKeeperAndInitDatabase(mode);
}

UInt64 DatabaseReplicated::getMetadataHash(const String & table_name) const
{
    return DB::getMetadataHash(table_name, readMetadataFile(table_name));
}

LoadTaskPtr DatabaseReplicated::startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode)
{
    auto base = DatabaseAtomic::startupDatabaseAsync(async_loader, std::move(startup_after), mode);
    auto job = makeLoadJob(
        base->goals(),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup Replicated database {}", getDatabaseName()),
        [this] (AsyncLoader &, const LoadJobPtr &)
        {
            UInt64 digest = 0;
            {
                std::lock_guard lock{mutex};
                for (const auto & table : tables)
                    digest += getMetadataHash(table.first);
                LOG_DEBUG(log, "Calculated metadata digest of {} tables: {}", tables.size(), digest);
            }

            {
                std::lock_guard lock{metadata_mutex};
                chassert(!tables_metadata_digest);
                tables_metadata_digest = digest;
            }

            if (is_probably_dropped)
                return;

            {
                std::lock_guard lock{ddl_worker_mutex};
                ddl_worker = std::make_unique<DatabaseReplicatedDDLWorker>(this, getContext());
                ddl_worker->startup();
                ddl_worker_initialized = true;
            }
        });
    std::scoped_lock lock(mutex);
    startup_replicated_database_task = makeLoadTask(async_loader, {job});
    return startup_replicated_database_task;
}

void DatabaseReplicated::waitDatabaseStarted() const
{
    LoadTaskPtr task;
    {
        std::scoped_lock lock(mutex);
        task = startup_replicated_database_task;
    }
    if (task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), task);
}

void DatabaseReplicated::stopLoading()
{
    LoadTaskPtr stop_startup_replicated_database;
    {
        std::scoped_lock lock(mutex);
        stop_startup_replicated_database.swap(startup_replicated_database_task);
    }
    stop_startup_replicated_database.reset();
    DatabaseAtomic::stopLoading();
}

void DatabaseReplicated::dumpLocalTablesForDebugOnly(const ContextPtr & local_context) const
{
    auto table_names = getAllTableNames(context.lock());
    for (const auto & table_name : table_names)
    {
        auto ast_ptr = tryGetCreateTableQuery(table_name, local_context);
        if (ast_ptr)
            LOG_DEBUG(log, "[local] Table {} create query is {}", table_name, queryToString(ast_ptr));
        else
            LOG_DEBUG(log, "[local] Table {} has no create query", table_name);
    }
}

void DatabaseReplicated::dumpTablesInZooKeeperForDebugOnly() const
{
    UInt32 max_log_ptr;
    auto table_name_to_metadata = tryGetConsistentMetadataSnapshot(getZooKeeper(), max_log_ptr);
    for (const auto & [table_name, create_table_query] : table_name_to_metadata)
    {
        auto query_ast = parseQueryFromMetadataInZooKeeper(table_name, create_table_query);
        if (query_ast)
        {
            LOG_DEBUG(log, "[zookeeper] Table {} create query is {}", table_name, queryToString(query_ast));
        }
        else
        {
            LOG_DEBUG(log, "[zookeeper] Table {} has no create query", table_name);
        }
    }
}

void DatabaseReplicated::tryCompareLocalAndZooKeeperTablesAndDumpDiffForDebugOnly(const ContextPtr & local_context) const
{
    UInt32 max_log_ptr;
    auto table_name_to_metadata_in_zk = tryGetConsistentMetadataSnapshot(getZooKeeper(), max_log_ptr);
    auto table_names_local = getAllTableNames(local_context);

    if (table_name_to_metadata_in_zk.size() != table_names_local.size())
        LOG_DEBUG(log, "Amount of tables in zk {} locally {}", table_name_to_metadata_in_zk.size(), table_names_local.size());

    std::unordered_set<std::string> checked_tables;

    for (const auto & table_name : table_names_local)
    {
        auto local_ast_ptr = tryGetCreateTableQuery(table_name, local_context);
        if (table_name_to_metadata_in_zk.contains(table_name))
        {
            checked_tables.insert(table_name);
            auto create_table_query_in_zk = table_name_to_metadata_in_zk[table_name];
            auto zk_ast_ptr = parseQueryFromMetadataInZooKeeper(table_name, create_table_query_in_zk);

            if (local_ast_ptr == nullptr && zk_ast_ptr == nullptr)
            {
                LOG_DEBUG(log, "AST for table {} is the same (nullptr) in local and ZK", table_name);
            }
            else if (local_ast_ptr != nullptr && zk_ast_ptr != nullptr && queryToString(local_ast_ptr) != queryToString(zk_ast_ptr))
            {
                LOG_DEBUG(log, "AST differs for table {}, local {}, in zookeeper {}", table_name, queryToString(local_ast_ptr), queryToString(zk_ast_ptr));
            }
            else if (local_ast_ptr == nullptr)
            {
                LOG_DEBUG(log, "AST differs for table {}, local nullptr, in zookeeper {}", table_name, queryToString(zk_ast_ptr));
            }
            else if (zk_ast_ptr == nullptr)
            {
                LOG_DEBUG(log, "AST differs for table {}, local {}, in zookeeper nullptr", table_name, queryToString(local_ast_ptr));
            }
            else
            {
                LOG_DEBUG(log, "AST for table {} is the same in local and ZK", table_name);
            }
        }
        else
        {
            if (local_ast_ptr == nullptr)
                LOG_DEBUG(log, "Table {} exists locally, but missing in ZK", table_name);
            else
                LOG_DEBUG(log, "Table {} exists locally with AST {}, but missing in ZK", table_name, queryToString(local_ast_ptr));
        }
    }
    for (const auto & [table_name, table_metadata] : table_name_to_metadata_in_zk)
    {
        if (!checked_tables.contains(table_name))
        {
            auto zk_ast_ptr = parseQueryFromMetadataInZooKeeper(table_name, table_metadata);
            if (zk_ast_ptr == nullptr)
                LOG_DEBUG(log, "Table {} exists in ZK with AST {}, but missing locally", table_name, queryToString(zk_ast_ptr));
            else
                LOG_DEBUG(log, "Table {} exists in ZK, but missing locally", table_name);
        }
    }
}

void DatabaseReplicated::checkTableEngine(const ASTCreateQuery & query, ASTStorage & storage, ContextPtr query_context) const
{
    bool replicated_table = storage.engine &&
        (startsWith(storage.engine->name, "Replicated") || startsWith(storage.engine->name, "Shared"));
    if (!replicated_table || !storage.engine->arguments)
        return;

    ASTs & args_ref = storage.engine->arguments->children;
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
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
    }

    ASTLiteral * arg1 = args[0]->as<ASTLiteral>();
    ASTLiteral * arg2 = args[1]->as<ASTLiteral>();
    if (!arg1 || !arg2 || arg1->value.getType() != Field::Types::String || arg2->value.getType() != Field::Types::String)
        return;

    String maybe_path = arg1->value.safeGet<String>();
    String maybe_replica = arg2->value.safeGet<String>();

    /// Looks like it's ReplicatedMergeTree with explicit zookeeper_path and replica_name arguments.
    /// Let's ensure that some macros are used.
    /// NOTE: we cannot check here that substituted values will be actually different on shards and replicas.

    Macros::MacroExpansionInfo info;
    info.table_id = {getDatabaseName(), query.getTable(), query.uuid};
    info.shard = getShardName();
    info.replica = getReplicaName();
    query_context->getMacros()->expand(maybe_path, info);
    bool maybe_shard_macros = info.expanded_other;
    info.expanded_other = false;
    query_context->getMacros()->expand(maybe_replica, info);
    bool maybe_replica_macros = info.expanded_other;
    bool enable_functional_tests_helper = getContext()->getConfigRef().has("_functional_tests_helper_database_replicated_replace_args_macros");

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

    /// We will replace it with default arguments if the setting is 2
    if (query_context->getSettingsRef()[Setting::database_replicated_allow_replicated_engine_arguments] != 2)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Explicit zookeeper_path and replica_name are specified in ReplicatedMergeTree arguments. "
                    "If you really want to specify it explicitly, then you should use some macros "
                    "to distinguish different shards and replicas");
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

#ifndef NDEBUG
        dumpLocalTablesForDebugOnly(local_context);
        dumpTablesInZooKeeperForDebugOnly();
        tryCompareLocalAndZooKeeperTablesAndDumpDiffForDebugOnly(local_context);
#endif

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
#ifndef NDEBUG
        dumpLocalTablesForDebugOnly(local_context);
        dumpTablesInZooKeeperForDebugOnly();
        tryCompareLocalAndZooKeeperTablesAndDumpDiffForDebugOnly(local_context);
#endif
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
            if (create->storage)
                checkTableEngine(*create, *create->storage, query_context);

            if (create->targets)
            {
                for (const auto & inner_table_engine : create->targets->getInnerEngines())
                    checkTableEngine(*create, *inner_table_engine, query_context);
            }
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
        if (query_drop->kind == ASTDropQuery::Kind::Detach
            && query_context->getSettingsRef()[Setting::database_replicated_always_detach_permanently])
            query_drop->permanently = true;
        if (query_drop->kind == ASTDropQuery::Kind::Detach && !query_drop->permanently)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "DETACH TABLE is not allowed for Replicated databases. "
                                                         "Use DETACH TABLE PERMANENTLY or SYSTEM RESTART REPLICA or set "
                                                         "database_replicated_always_detach_permanently to 1");
    }
}

BlockIO DatabaseReplicated::tryEnqueueReplicatedDDL(const ASTPtr & query, ContextPtr query_context, QueryFlags flags)
{
    waitDatabaseStarted();

    if (!DatabaseCatalog::instance().canPerformReplicatedDDLQueries())
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Replicated DDL queries are disabled");

    if (query_context->getCurrentTransaction() && query_context->getSettingsRef()[Setting::throw_on_unsupported_query_inside_transaction])
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Distributed DDL queries inside transactions are not supported");

    if (is_readonly)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "Database is in readonly mode, because it cannot connect to ZooKeeper");

    if (!flags.internal && (query_context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "It's not initial query. ON CLUSTER is not allowed for Replicated database.");

    checkQueryValid(query, query_context);
    LOG_DEBUG(log, "Proposing query: {}", queryToString(query));

    DDLLogEntry entry;
    entry.query = queryToString(query);
    entry.initiator = ddl_worker->getCommonHostID();
    entry.setSettingsIfRequired(query_context);
    entry.tracing_context = OpenTelemetry::CurrentContext();
    entry.is_backup_restore = flags.distributed_backup_restore;
    String node_path = ddl_worker->tryEnqueueAndExecuteEntry(entry, query_context);

    Strings hosts_to_wait;
    Strings unfiltered_hosts = getZooKeeper()->getChildren(zookeeper_path + "/replicas");

    std::vector<String> paths;
    for (const auto & host : unfiltered_hosts)
        paths.push_back(zookeeper_path + "/replicas/" + host + "/replica_group");

    auto replica_groups = getZooKeeper()->tryGet(paths);

    for (size_t i = 0; i < paths.size(); ++i)
    {
        if (replica_groups[i].data == replica_group_name)
            hosts_to_wait.push_back(unfiltered_hosts[i]);
    }


    return getQueryStatus(node_path, fs::path(zookeeper_path) / "replicas", query_context, hosts_to_wait);
}

static UUID getTableUUIDIfReplicated(const String & metadata, ContextPtr context)
{
    bool looks_like_replicated = metadata.find("Replicated") != std::string::npos;
    bool looks_like_shared = metadata.find("Shared") != std::string::npos;
    bool looks_like_merge_tree = metadata.find("MergeTree") != std::string::npos;
    if (!(looks_like_replicated || looks_like_shared) || !looks_like_merge_tree)
        return UUIDHelpers::Nil;

    ParserCreateQuery parser;
    auto size = context->getSettingsRef()[Setting::max_query_size];
    auto depth = context->getSettingsRef()[Setting::max_parser_depth];
    auto backtracks = context->getSettingsRef()[Setting::max_parser_backtracks];
    ASTPtr query = parseQuery(parser, metadata, size, depth, backtracks);
    const ASTCreateQuery & create = query->as<const ASTCreateQuery &>();
    if (!create.storage || !create.storage->engine)
        return UUIDHelpers::Nil;
    if (!(startsWith(create.storage->engine->name, "Replicated") || startsWith(create.storage->engine->name, "Shared"))
        || !endsWith(create.storage->engine->name, "MergeTree"))
        return UUIDHelpers::Nil;
    chassert(create.uuid != UUIDHelpers::Nil);
    return create.uuid;
}

void DatabaseReplicated::recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 & max_log_ptr)
{
    waitDatabaseStarted();

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
    for (auto existing_tables_it = getTablesIterator(getContext(), {}, /*skip_not_loaded=*/false); existing_tables_it->isValid();
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
        enableAllExperimentalSettings(query_context);

        query_context->setSetting("database_replicated_allow_explicit_uuid", 3);
        query_context->setSetting("database_replicated_allow_replicated_engine_arguments", 3);

        auto txn = std::make_shared<ZooKeeperMetadataTransaction>(current_zookeeper, zookeeper_path, false, "");
        query_context->initZooKeeperMetadataTransaction(txn);
        return query_context;
    };

    String db_name = getDatabaseName();
    String to_db_name = getDatabaseName() + BROKEN_TABLES_SUFFIX;
    String to_db_name_replicated = getDatabaseName() + BROKEN_REPLICATED_TABLES_SUFFIX;
    if (total_tables * db_settings[DatabaseReplicatedSetting::max_broken_tables_ratio] < tables_to_detach.size())
        throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "Too many tables to recreate: {} of {}", tables_to_detach.size(), total_tables);
    if (!tables_to_detach.empty())
    {
        LOG_WARNING(log, "Will recreate {} broken tables to recover replica", tables_to_detach.size());
        /// It's too dangerous to automatically drop tables, so we will move them to special database.
        /// We use Ordinary engine for destination database, because it's the only way to discard table UUID
        /// and make possible creation of new table with the same UUID.
        String query = fmt::format("CREATE DATABASE IF NOT EXISTS {} ENGINE=Ordinary", backQuoteIfNeed(to_db_name));
        auto query_context = Context::createCopy(getContext());
        query_context->setSetting("allow_deprecated_database_ordinary", 1);
        query_context->setSetting("cloud_mode", false);
        executeQuery(query, query_context, QueryFlags{ .internal = true });

        /// But we want to avoid discarding UUID of ReplicatedMergeTree tables, because it will not work
        /// if zookeeper_path contains {uuid} macro. Replicated database do not recreate replicated tables on recovery,
        /// so it's ok to save UUID of replicated table.
        query = fmt::format("CREATE DATABASE IF NOT EXISTS {} ENGINE=Atomic", backQuoteIfNeed(to_db_name_replicated));
        query_context = Context::createCopy(getContext());
        query_context->setSetting("cloud_mode", false);
        executeQuery(query, query_context, QueryFlags{ .internal = true });
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

    LOG_DEBUG(log, "Renames completed successfully");

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
        tables_dependencies.addDependencies(qualified_name, getDependenciesFromCreateQuery(getContext()->getGlobalContext(), qualified_name, query_ast, getContext()->getCurrentDatabase()));
    }

    tables_dependencies.checkNoCyclicDependencies();

    auto allow_concurrent_table_creation = getContext()->getServerSettings()[ServerSetting::max_database_replicated_create_table_thread_pool_size] > 1;
    auto tables_to_create_by_level = tables_dependencies.getTablesSplitByDependencyLevel();

    ThreadPoolCallbackRunnerLocal<void> runner(getDatabaseReplicatedCreateTablesThreadPool().get(), "CreateTables");

    for (const auto & tables_to_create : tables_to_create_by_level)
    {
        for (const auto & table_id : tables_to_create)
        {
            auto task = [&]()
            {
                auto table_name = table_id.getTableName();
                auto metadata_it = table_name_to_metadata.find(table_name);
                if (metadata_it == table_name_to_metadata.end())
                {
                    /// getTablesSortedByDependency() may return some not existing tables or tables from other databases
                    LOG_WARNING(log, "Got table name {} when resolving table dependencies, "
                                "but database {} does not have metadata for that table. Ignoring it", table_id.getNameForLogs(), getDatabaseName());
                    return;
                }

                const auto & create_query_string = metadata_it->second;
                if (isTableExist(table_name, getContext()))
                {
                    assert(create_query_string == readMetadataFile(table_name) || getTableUUIDIfReplicated(create_query_string, getContext()) != UUIDHelpers::Nil);
                    return;
                }

                auto query_ast = parseQueryFromMetadataInZooKeeper(table_name, create_query_string);
                LOG_INFO(log, "Executing {}", serializeAST(*query_ast));
                auto create_query_context = make_query_context();
                InterpreterCreateQuery(query_ast, create_query_context).execute();
            };

            if (allow_concurrent_table_creation)
                runner(std::move(task));
            else
                task();
        }

        runner.waitForAllToFinishAndRethrowFirstError();
    }
    LOG_INFO(log, "All tables are created successfully");

    UInt32 first_entry_to_mark_finished = new_replica ? max_log_ptr_at_creation : our_log_ptr;
    /// NOTE first_entry_to_mark_finished can be 0 if our replica has crashed just after creating its nodes in ZK,
    /// so it's a new replica, but after restarting we don't know max_log_ptr_at_creation anymore...
    /// It's a very rare case, and it's okay if some queries throw TIMEOUT_EXCEEDED when waiting for all replicas
    if (first_entry_to_mark_finished)
    {
        /// If the replica is new and some of the queries applied during recovery
        /// where issued after the replica was created, then other nodes might be
        /// waiting for this node to notify them that the query was applied.
        for (UInt32 ptr = first_entry_to_mark_finished; ptr <= max_log_ptr; ++ptr)
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

std::map<String, String> DatabaseReplicated::tryGetConsistentMetadataSnapshot(const ZooKeeperPtr & zookeeper, UInt32 & max_log_ptr) const
{
    return getConsistentMetadataSnapshotImpl(zookeeper, {}, /* max_retries= */ 10, max_log_ptr);
}

std::map<String, String> DatabaseReplicated::getConsistentMetadataSnapshotImpl(
    const ZooKeeperPtr & zookeeper,
    const FilterByNameFunction & filter_by_table_name,
    size_t max_retries,
    UInt32 & max_log_ptr) const
{
    std::map<String, String> table_name_to_metadata;
    size_t iteration = 0;
    while (++iteration <= max_retries)
    {
        table_name_to_metadata.clear();
        LOG_DEBUG(log, "Trying to get consistent metadata snapshot for log pointer {}", max_log_ptr);

        Strings escaped_table_names;
        escaped_table_names = zookeeper->getChildren(zookeeper_path + "/metadata");
        if (filter_by_table_name)
            std::erase_if(escaped_table_names, [&](const String & table) { return !filter_by_table_name(unescapeForFileName(table)); });

        std::vector<zkutil::ZooKeeper::FutureGet> futures;
        futures.reserve(escaped_table_names.size());
        for (const auto & table : escaped_table_names)
            futures.emplace_back(zookeeper->asyncTryGet(zookeeper_path + "/metadata/" + table));

        for (size_t i = 0; i < escaped_table_names.size(); ++i)
        {
            auto res = futures[i].get();
            if (res.error != Coordination::Error::ZOK)
                break;
            table_name_to_metadata.emplace(unescapeForFileName(escaped_table_names[i]), res.data);
        }

        UInt32 new_max_log_ptr = parse<UInt32>(zookeeper->get(zookeeper_path + "/max_log_ptr"));
        if (new_max_log_ptr == max_log_ptr && escaped_table_names.size() == table_name_to_metadata.size())
            break;

        if (max_log_ptr < new_max_log_ptr)
        {
            LOG_DEBUG(log, "Log pointer moved from {} to {}, will retry", max_log_ptr, new_max_log_ptr);
            max_log_ptr = new_max_log_ptr;
        }
        else
        {
            chassert(max_log_ptr == new_max_log_ptr);
            chassert(escaped_table_names.size() != table_name_to_metadata.size());
            LOG_DEBUG(log, "Cannot get metadata of some tables due to ZooKeeper error, will retry");
        }
    }

    if (max_retries < iteration)
        throw Exception(ErrorCodes::CANNOT_GET_REPLICATED_DATABASE_SNAPSHOT, "Cannot get consistent metadata snapshot");

    LOG_DEBUG(log, "Got consistent metadata snapshot for log pointer {}", max_log_ptr);

    return table_name_to_metadata;
}

ASTPtr DatabaseReplicated::parseQueryFromMetadataInZooKeeper(const String & node_name, const String & query) const
{
    ParserCreateQuery parser;
    String description = "in ZooKeeper " + zookeeper_path + "/metadata/" + node_name;
    auto ast = parseQuery(
        parser,
        query,
        description,
        0,
        getContext()->getSettingsRef()[Setting::max_parser_depth],
        getContext()->getSettingsRef()[Setting::max_parser_backtracks]);

    auto & create = ast->as<ASTCreateQuery &>();
    if (create.uuid == UUIDHelpers::Nil || create.getTable() != TABLE_WITH_UUID_NAME_PLACEHOLDER || create.database)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected query from {}: {}", node_name, query);

    create.setDatabase(getDatabaseName());
    create.setTable(unescapeForFileName(node_name));
    create.attach = create.is_materialized_view_with_inner_table();

    return ast;
}

void DatabaseReplicated::dropReplica(
    DatabaseReplicated * database, const String & database_zookeeper_path, const String & shard, const String & replica, bool throw_if_noop)
{
    assert(!database || database_zookeeper_path == database->zookeeper_path);

    String full_replica_name = shard.empty() ? replica : getFullReplicaName(shard, replica);

    if (full_replica_name.find('/') != std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid replica name, '/' is not allowed: {}", full_replica_name);

    auto zookeeper = Context::getGlobalContextInstance()->getZooKeeper();

    String database_mark;
    bool db_path_exists = zookeeper->tryGet(database_zookeeper_path, database_mark);
    if (!db_path_exists && !throw_if_noop)
        return;
    if (database_mark != REPLICATED_DATABASE_MARK)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} does not look like a path of Replicated database", database_zookeeper_path);

    String database_replica_path = fs::path(database_zookeeper_path) / "replicas" / full_replica_name;
    if (!zookeeper->exists(database_replica_path))
    {
        if (!throw_if_noop)
            return;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica {} does not exist (database path: {})",
                        full_replica_name, database_zookeeper_path);
    }

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

    waitDatabaseStarted();

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

void DatabaseReplicated::renameDatabase(ContextPtr query_context, const String & new_name)
{
    DatabaseAtomic::renameDatabase(query_context, new_name);
    auto db_name_path = fs::path(zookeeper_path) / FIRST_REPLICA_DATABASE_NAME;
    getZooKeeper()->set(db_name_path, getDatabaseName());
}

void DatabaseReplicated::stopReplication()
{
    std::lock_guard lock{ddl_worker_mutex};
    if (ddl_worker)
        ddl_worker->shutdown();
}

void DatabaseReplicated::shutdown()
{
    stopReplication();
    {
        std::lock_guard lock{ddl_worker_mutex};
        ddl_worker_initialized = false;
        ddl_worker = nullptr;
    }
    DatabaseAtomic::shutdown();
}


void DatabaseReplicated::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    waitDatabaseStarted();

    auto txn = local_context->getZooKeeperMetadataTransaction();
    assert(!ddl_worker || !ddl_worker->isCurrentlyActive() || txn || startsWith(table_name, ".inner_id.") || startsWith(table_name, ".tmp.inner_id."));
    if (txn && txn->isInitialQuery() && !txn->isCreateOrReplaceQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(table_name);
        txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));
    }

    auto table = tryGetTable(table_name, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} doesn't exist", table_name);
    if (table->getName() == "MaterializedView" || table->getName() == "WindowView" || table->getName() == "SharedSet" || table->getName() == "SharedJoin")
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

    waitDatabaseStarted();

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
    std::lock_guard lock{ddl_worker_mutex};
    return ddl_worker_initialized && ddl_worker->isCurrentlyActive();
}

void DatabaseReplicated::detachTablePermanently(ContextPtr local_context, const String & table_name)
{
    waitDatabaseStarted();

    if (!local_context->getServerSettings()[ServerSetting::database_replicated_allow_detach_permanently])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Support for DETACH TABLE PERMANENTLY is disabled");

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
    waitDatabaseStarted();

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
    waitDatabaseStarted();

    /// Here we read metadata from ZooKeeper. We could do that by simple call of DatabaseAtomic::getTablesForBackup() however
    /// reading from ZooKeeper is better because thus we won't be dependent on how fast the replication queue of this database is.
    auto zookeeper = getContext()->getZooKeeper();
    UInt32 snapshot_version = parse<UInt32>(zookeeper->get(zookeeper_path + "/max_log_ptr"));
    auto snapshot = getConsistentMetadataSnapshotImpl(zookeeper, filter, /* max_retries= */ 20, snapshot_version);

    std::vector<std::pair<ASTPtr, StoragePtr>> res;
    for (const auto & [table_name, metadata] : snapshot)
    {
        ParserCreateQuery parser;
        auto create_table_query = parseQuery(
            parser, metadata, 0, getContext()->getSettingsRef()[Setting::max_parser_depth], getContext()->getSettingsRef()[Setting::max_parser_backtracks]);

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
    waitDatabaseStarted();

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

void registerDatabaseReplicated(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replicated database requires 3 arguments: zookeeper path, shard name and replica name");

        auto & arguments = engine->arguments->children;
        for (auto & engine_arg : arguments)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.context);

        String zookeeper_path = safeGetLiteralValue<String>(arguments[0], "Replicated");
        String shard_name = safeGetLiteralValue<String>(arguments[1], "Replicated");
        String replica_name  = safeGetLiteralValue<String>(arguments[2], "Replicated");

        /// Expand macros.
        Macros::MacroExpansionInfo info;
        info.table_id.database_name = args.database_name;
        info.table_id.uuid = args.uuid;
        zookeeper_path = args.context->getMacros()->expand(zookeeper_path, info);

        info.level = 0;
        info.table_id.uuid = UUIDHelpers::Nil;
        shard_name = args.context->getMacros()->expand(shard_name, info);

        info.level = 0;
        replica_name = args.context->getMacros()->expand(replica_name, info);

        DatabaseReplicatedSettings database_replicated_settings{};
        if (engine_define->settings)
            database_replicated_settings.loadFromQuery(*engine_define);

        return std::make_shared<DatabaseReplicated>(
            args.database_name,
            args.metadata_path,
            args.uuid,
            zookeeper_path,
            shard_name,
            replica_name,
            std::move(database_replicated_settings), args.context);
    };
    factory.registerDatabase("Replicated", create_fn, {.supports_arguments = true, .supports_settings = true});
}

BlockIO DatabaseReplicated::getQueryStatus(
    const String & node_path, const String & replicas_path, ContextPtr context_, const Strings & hosts_to_wait)
{
    BlockIO io;
    if (context_->getSettingsRef()[Setting::distributed_ddl_task_timeout] == 0)
        return io;

    auto source = std::make_shared<ReplicatedDatabaseQueryStatusSource>(node_path, replicas_path, context_, hosts_to_wait);
    io.pipeline = QueryPipeline(std::move(source));

    if (context_->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE
        || context_->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE_ONLY_ACTIVE)
        io.pipeline.complete(std::make_shared<EmptySink>(io.pipeline.getHeader()));

    return io;
}
}
