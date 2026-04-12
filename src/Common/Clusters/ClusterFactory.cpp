#include <Common/Clusters/ClusterFactory.h>

#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/quoteString.h>
#include <Common/parseAddress.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Core/BackgroundSchedulePool.h>
#include <base/sleep.h>
#include <Parsers/ASTAlterClusterQuery.h>
#include <Parsers/ASTAlterShardQuery.h>
#include <Parsers/ASTCreateClusterQuery.h>
#include <Parsers/ASTCreateShardQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/string/trim.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int SHARD_ALREADY_EXISTS;
    extern const int SHARD_DOESNT_EXIST;
    extern const int SHARD_IS_REFERENCED;
    extern const int CLUSTER_DEFINITION_ALREADY_EXISTS;
    extern const int CLUSTER_DEFINITION_DOESNT_EXIST;
    extern const int BAD_CLUSTER_DEFINITION;
    extern const int CLUSTER_DEFINITION_NAME_AMBIGUOUS;
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr char REPLICAS_LIST_SEPARATOR = '|';

const String NC_REPLICAS{"replicas"};
const String NC_HOST{"host"};
const String NC_PORT{"port"};
const String NC_USER{"user"};
const String NC_PASSWORD{"password"};
const String NC_DEFAULT_USER{"default"};
const String NC_DEFAULT_DATABASE{"default_database"};
const String NC_BIND_HOST{"bind_host"};
const String NC_PRIORITY{"priority"};
const String NC_SECURE{"secure"};
const String NC_WEIGHT{"weight"};
const String NC_INTERNAL_REPLICATION{"internal_replication"};

constexpr UInt64 NC_NUM_DEFAULT_OFF = 0;
constexpr UInt64 NC_NUM_DEFAULT_PRIORITY = 1;
constexpr UInt64 NC_NUM_DEFAULT_WEIGHT = 1;

const String EMPTY_STRING;

String formatCreateShardStatement(
    const String & shard_name,
    const std::vector<String> & replica_collections,
    UInt32 weight,
    bool internal_replication)
{
    ASTCreateShardQuery ast;
    ast.shard_name = shard_name;
    ast.replicas = replica_collections;
    ast.shard_properties.clear();
    ast.shard_properties.push_back(SettingChange{"weight", Field{UInt64{weight}}});
    ast.shard_properties.push_back(SettingChange{"internal_replication", Field{internal_replication}});
    ast.if_not_exists = false;
    return ast.formatWithSecretsOneLine();
}

String formatCreateClusterStatement(
    const String & cluster_name,
    const std::vector<String> & members,
    const String & cluster_secret,
    bool allow_distributed_ddl_queries)
{
    ASTCreateClusterQuery ast;
    ast.cluster_name = cluster_name;
    ast.members = members;
    if (!cluster_secret.empty())
        ast.cluster_properties.push_back(SettingChange{"secret", Field{cluster_secret}});
    /// Always persist `allow_distributed_ddl_queries` so `SHOW CREATE CLUSTER` matches catalog state (default is true).
    ast.cluster_properties.push_back(SettingChange{"allow_distributed_ddl_queries", Field{allow_distributed_ddl_queries}});
    ast.if_not_exists = false;
    return ast.formatWithSecretsOneLine();
}

template <typename T>
std::optional<T> tryGetNamedCollectionScalar(const NamedCollection & coll, const String & key)
{
    try
    {
        return coll.get<T>(key);
    }
    catch (const Exception &)
    {
        return std::nullopt;
    }
}

/// Read an unsigned integer from a named collection key, same spirit as `S3RequestSettings` construction from `NamedCollection`:
/// stored type may be `UInt64`, `Int64`, `bool`, `Float64`, or decimal `String`.
UInt64 getUIntField(const NamedCollection & coll, const String & key, UInt64 default_value)
{
    if (!coll.has(key))
        return default_value;

    if (auto v = tryGetNamedCollectionScalar<UInt64>(coll, key))
        return *v;
    if (auto v = tryGetNamedCollectionScalar<Int64>(coll, key))
    {
        if (*v < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Named collection key `{}` must be non-negative", key);
        return static_cast<UInt64>(*v);
    }
    if (auto v = tryGetNamedCollectionScalar<bool>(coll, key))
        return static_cast<UInt64>(*v);
    if (auto v = tryGetNamedCollectionScalar<Float64>(coll, key))
        return static_cast<UInt64>(*v);
    if (auto v = tryGetNamedCollectionScalar<String>(coll, key))
        return parse<UInt64>(*v);

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read unsigned integer from named collection for key `{}`", key);
}

struct SqlCatalogReplicaHostPort
{
    String host;
    UInt16 port = 0;
    bool valid = false;
};

/// Host:port pair for SQL catalog duplicate-endpoint checks. Skips whole-shard collections (`replicas` key).
SqlCatalogReplicaHostPort tryExtractSqlCatalogReplicaHostPort(const NamedCollection & coll)
{
    SqlCatalogReplicaHostPort out;
    if (coll.has(NC_REPLICAS) || !coll.has(NC_HOST))
        return out;
    out.host = coll.get<String>(NC_HOST);
    out.port = static_cast<UInt16>(coll.getOrDefault<UInt64>(NC_PORT, NC_NUM_DEFAULT_OFF));
    out.valid = out.port != 0;
    return out;
}

Cluster::Address makeReplicaAddress(
    const String & nc_name,
    const NamedCollection & coll,
    const String & cluster_name,
    const String & cluster_secret,
    UInt32 shard_index,
    UInt32 replica_index,
    UInt16 clickhouse_port)
{
    if (coll.has(NC_REPLICAS))
        throw Exception(
            ErrorCodes::BAD_CLUSTER_DEFINITION,
            "Replica named collection `{}` must not contain `replicas` key (whole-shard collections only)",
            nc_name);

    Cluster::Address addr;
    addr.host_name = coll.get<String>(NC_HOST);
    addr.port = static_cast<UInt16>(coll.getOrDefault<UInt64>(NC_PORT, NC_NUM_DEFAULT_OFF));
    if (!addr.port)
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Replica named collection `{}` requires positive `port`", nc_name);

    addr.user = coll.getOrDefault<String>(NC_USER, NC_DEFAULT_USER);
    addr.password = coll.getOrDefault<String>(NC_PASSWORD, EMPTY_STRING);
    addr.default_database = coll.getOrDefault<String>(NC_DEFAULT_DATABASE, EMPTY_STRING);
    addr.bind_host = coll.getOrDefault<String>(NC_BIND_HOST, EMPTY_STRING);
    addr.priority = Priority{static_cast<int>(getUIntField(coll, NC_PRIORITY, NC_NUM_DEFAULT_PRIORITY))};
    bool sec = getUIntField(coll, NC_SECURE, NC_NUM_DEFAULT_OFF) != 0;
    addr.secure = sec ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    addr.shard_index = shard_index;
    addr.replica_index = replica_index;
    addr.cluster = cluster_name;
    addr.cluster_secret = cluster_secret;
    addr.user_specified = coll.has(NC_USER);
    addr.recomputeIsLocal(clickhouse_port);
    addr.compression = addr.is_local ? Protocol::Compression::Disable : Protocol::Compression::Enable;
    return addr;
}

Cluster::ShardInitSpec makeWholeShardSpec(
    const String & nc_name,
    const NamedCollection & coll,
    const String & cluster_name,
    const String & cluster_secret,
    UInt32 shard_index,
    UInt16 clickhouse_port,
    UInt16 default_port)
{
    if (!coll.has(NC_REPLICAS))
        throw Exception(
            ErrorCodes::BAD_CLUSTER_DEFINITION,
            "Named collection `{}` is used as a whole shard in SQL cluster but has no `replicas` key",
            nc_name);

    String replicas_line = coll.get<String>(NC_REPLICAS);

    String user = coll.getOrDefault<String>(NC_USER, NC_DEFAULT_USER);
    String password = coll.getOrDefault<String>(NC_PASSWORD, EMPTY_STRING);
    UInt32 weight = static_cast<UInt32>(getUIntField(coll, NC_WEIGHT, NC_NUM_DEFAULT_WEIGHT));
    bool internal_replication = getUIntField(coll, NC_INTERNAL_REPLICATION, NC_NUM_DEFAULT_OFF) != 0;
    bool sec = getUIntField(coll, NC_SECURE, NC_NUM_DEFAULT_OFF) != 0;
    String default_database = coll.getOrDefault<String>(NC_DEFAULT_DATABASE, EMPTY_STRING);
    String bind_host = coll.getOrDefault<String>(NC_BIND_HOST, EMPTY_STRING);
    Priority priority{static_cast<int>(getUIntField(coll, NC_PRIORITY, NC_NUM_DEFAULT_PRIORITY))};

    Cluster::Addresses addresses;
    UInt32 replica_index = 1;
    for (size_t start = 0; start < replicas_line.size();)
    {
        size_t sep = replicas_line.find(REPLICAS_LIST_SEPARATOR, start);
        String part = replicas_line.substr(start, sep == String::npos ? String::npos : sep - start);
        boost::trim(part);
        if (!part.empty())
        {
            auto host_port = parseAddress(part, default_port);
            Cluster::Address addr;
            addr.host_name = host_port.first;
            addr.port = host_port.second;
            addr.user = user;
            addr.password = password;
            addr.default_database = default_database;
            addr.bind_host = bind_host;
            addr.priority = priority;
            addr.secure = sec ? Protocol::Secure::Enable : Protocol::Secure::Disable;
            addr.shard_index = shard_index;
            addr.replica_index = replica_index++;
            addr.cluster = cluster_name;
            addr.cluster_secret = cluster_secret;
            addr.user_specified = coll.has(NC_USER);
            addr.recomputeIsLocal(clickhouse_port);
            addr.compression = addr.is_local ? Protocol::Compression::Disable : Protocol::Compression::Enable;
            addresses.push_back(std::move(addr));
        }
        if (sep == String::npos)
            break;
        start = sep + 1;
    }

    if (addresses.empty())
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "No valid replica endpoints in `replicas` for `{}`", nc_name);

    return Cluster::ShardInitSpec{std::move(addresses), weight, internal_replication};
}

}

ClusterFactory & ClusterFactory::instance()
{
    static ClusterFactory factory;
    return factory;
}

ClusterFactory::~ClusterFactory()
{
    shutdown();
}

void ClusterFactory::reloadSqlDefinitionsLocked()
{
    loaded_sql_shards = shards_metadata_storage->getAll();
    loaded_sql_clusters = clusters_metadata_storage->getAll();

    LOG_INFO(
        log,
        "Reloaded SQL catalog definitions from metadata storage: {} shard(s), {} cluster(s)",
        loaded_sql_shards.size(),
        loaded_sql_clusters.size());

    for (const auto & [shard_name, shard_def] : loaded_sql_shards)
    {
        String replica_collections_str;
        for (size_t i = 0; i < shard_def.replica_collections.size(); ++i)
        {
            if (i)
                replica_collections_str += ", ";
            replica_collections_str += "`" + shard_def.replica_collections[i] + "`";
        }
        LOG_INFO(
            log,
            "SQL shard `{}`: weight={}, internal_replication={}, replica_named_collections=[{}]",
            shard_name,
            shard_def.weight,
            shard_def.internal_replication,
            replica_collections_str);
    }

    for (const auto & [cluster_name, cluster_def] : loaded_sql_clusters)
    {
        String members_str;
        for (size_t i = 0; i < cluster_def.members.size(); ++i)
        {
            if (i)
                members_str += ", ";
            members_str += "`" + cluster_def.members[i] + "`";
        }
        LOG_INFO(log, "SQL cluster `{}`: members=[{}]", cluster_name, members_str);
    }

    rebuildSqlClusterRegistrationsLocked();
}

void ClusterFactory::rebuildSqlClusterRegistrationsLocked()
{
    std::erase_if(cluster_registrations, [](const auto & entry)
    {
        return entry.second.source == ClusterDefinitionSource::SqlCatalog;
    });

    UInt64 version = ++sql_catalog_mutation_counter;
    for (const auto & [name, _] : loaded_sql_clusters)
    {
        auto reg_it = cluster_registrations.find(name);
        if (reg_it != cluster_registrations.end() && reg_it->second.source == ClusterDefinitionSource::RemoteServersConfig)
            continue;
        cluster_registrations[name] = ClusterDefinitionRegistration{ClusterDefinitionSource::SqlCatalog, version};
    }
}

void ClusterFactory::reloadFromConfig(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix, UInt64 remote_servers_definition_version)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    std::lock_guard lock(mutex);

    std::erase_if(cluster_registrations, [](const auto & entry)
    {
        return entry.second.source == ClusterDefinitionSource::RemoteServersConfig;
    });

    for (const auto & key : keys)
    {
        if (key.contains('.'))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Cluster names with dots are not supported: '{}'", key);

        if (config.has(config_prefix + "." + key + ".discovery"))
            continue;

        cluster_registrations[key]
            = ClusterDefinitionRegistration{ClusterDefinitionSource::RemoteServersConfig, remote_servers_definition_version};
    }
}

void ClusterFactory::initialize(const String & data_path)
{
    std::lock_guard lock(mutex);
    if (initialized)
        return;

    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::initialize called before global context exists");

    const String metadata_root = (std::filesystem::path(data_path) / "cluster_metadata").string();
    shards_metadata_storage = ShardsMetadataStorage::create(
        global_context, (std::filesystem::path(metadata_root) / "shards").string());
    clusters_metadata_storage = ClustersMetadataStorage::create(
        global_context, (std::filesystem::path(metadata_root) / "clusters").string());
    reloadSqlDefinitionsLocked();
    initialized = true;
    LOG_INFO(log, "ClusterFactory initialized at {}", metadata_root);

    if ((shards_metadata_storage->isReplicated() || clusters_metadata_storage->isReplicated()) && !sql_catalog_update_task)
    {
        sql_catalog_update_task = global_context->getSchedulePool().createTask(
            StorageID::createEmpty(),
            "ClusterFactoryClusterCatalogBackend",
            [this]
            {
                updateFunc();
            });
        sql_catalog_update_task->activate();
        sql_catalog_update_task->schedule();
    }
}

void ClusterFactory::shutdown()
{
    shutdown_called.store(true);
    if (sql_catalog_update_task)
        sql_catalog_update_task->deactivate();
    std::lock_guard lock(mutex);
    if (shards_metadata_storage)
        shards_metadata_storage->shutdown();
    if (clusters_metadata_storage)
        clusters_metadata_storage->shutdown();
    sql_catalog_update_task = {};
    shards_metadata_storage.reset();
    clusters_metadata_storage.reset();
    initialized = false;
    loaded_sql_shards.clear();
    loaded_sql_clusters.clear();
    cluster_registrations.clear();
    sql_catalog_mutation_counter = 0;
}

void ClusterFactory::reloadFromSQL()
{
    std::lock_guard lock(mutex);
    if (!initialized || !shards_metadata_storage || !clusters_metadata_storage)
        return;
    reloadSqlDefinitionsLocked();
}

void ClusterFactory::updateFunc()
{
    LOG_TRACE(log, "SQL shard/cluster catalog background update thread started");

    while (!shutdown_called.load())
    {
        try
        {
            bool need_reload = false;
            if (shards_metadata_storage && shards_metadata_storage->isReplicated())
                need_reload |= shards_metadata_storage->waitCatalogUpdate();
            if (clusters_metadata_storage && clusters_metadata_storage->isReplicated())
                need_reload |= clusters_metadata_storage->waitCatalogUpdate();
            if (need_reload)
                reloadFromSQL();
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                LOG_INFO(
                    log,
                    "Lost ZooKeeper connection while watching SQL shard/cluster catalog, will retry: {}",
                    DB::getCurrentExceptionMessage(true));
                sleepForSeconds(1);
            }
            else
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                chassert(false);
            }
            continue;
        }
        catch (...)
        {
            /// Ok: unexpected non-coordination errors from catalog watch/reload; logged, then loop continues until shutdown.
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
            continue;
        }
    }

    LOG_TRACE(log, "SQL shard/cluster catalog background update thread finished");
}

void ClusterFactory::createShard(
    const String & shard_name,
    const std::vector<String> & replica_collections,
    UInt32 weight,
    bool internal_replication)
{
    if (replica_collections.empty())
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "CREATE SHARD requires at least one replica collection");

    loadNamedCollectionsIfNeeded();

    if (namedCollectionExists(shard_name))
        throw Exception(
            ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS,
            "Cannot create SQL SHARD `{}` because a named collection with the same name already exists",
            shard_name);

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (loaded_sql_shards.contains(shard_name))
        throw Exception(ErrorCodes::SHARD_ALREADY_EXISTS, "SQL SHARD `{}` already exists", shard_name);

    if (loaded_sql_clusters.contains(shard_name))
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL CLUSTER", shard_name);

    ShardCatalogDefinition record;
    record.replica_collections = replica_collections;
    record.weight = weight;
    record.internal_replication = internal_replication;

    for (const auto & rep : replica_collections)
    {
        if (!namedCollectionExists(rep))
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Replica named collection `{}` does not exist", rep);
    }

    String create_sql = formatCreateShardStatement(shard_name, replica_collections, weight, internal_replication);
    shards_metadata_storage->writeCreateStatement(shard_name, create_sql, false);
    loaded_sql_shards[shard_name] = std::move(record);
}

void ClusterFactory::dropShard(const String & shard_name, bool if_exists)
{
    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    auto it = loaded_sql_shards.find(shard_name);
    if (it == loaded_sql_shards.end())
    {
        if (!if_exists)
            throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "SQL SHARD `{}` does not exist", shard_name);
        return;
    }

    for (const auto & [cname, crec] : loaded_sql_clusters)
    {
        for (const auto & m : crec.members)
        {
            if (m == shard_name)
                throw Exception(
                    ErrorCodes::SHARD_IS_REFERENCED,
                    "Cannot drop SQL SHARD `{}` because SQL CLUSTER `{}` references it",
                    shard_name,
                    cname);
        }
    }

    LOG_INFO(log, "Removing SQL SHARD catalog definition for `{}` from shards catalog storage", shard_name);
    shards_metadata_storage->remove(shard_name);
    LOG_INFO(log, "Removed SQL SHARD catalog definition for `{}` from shards catalog storage", shard_name);
    loaded_sql_shards.erase(it);
}

void ClusterFactory::checkSqlClusterMemberNameLocked(const String & m) const
{
    if (loaded_sql_shards.contains(m))
        return;
    if (!namedCollectionExists(m))
        throw Exception(
            ErrorCodes::BAD_CLUSTER_DEFINITION,
            "SQL CLUSTER member `{}` is neither an existing SQL SHARD nor a named collection",
            m);
    auto coll = getNamedCollection(m);
    if (!coll->has(NC_REPLICAS))
        throw Exception(
            ErrorCodes::BAD_CLUSTER_DEFINITION,
            "Named collection `{}` cannot be used as a whole shard in SQL CLUSTER because it has no `replicas` key",
            m);
}

void ClusterFactory::createCluster(
    const String & cluster_name,
    const std::vector<String> & members,
    const String & cluster_secret,
    bool allow_distributed_ddl_queries)
{
    if (members.empty())
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "CREATE CLUSTER requires at least one shard member");

    loadNamedCollectionsIfNeeded();

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (loaded_sql_clusters.contains(cluster_name))
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_ALREADY_EXISTS, "SQL CLUSTER `{}` already exists", cluster_name);

    if (loaded_sql_shards.contains(cluster_name))
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL SHARD", cluster_name);

    if (namedCollectionExists(cluster_name))
        throw Exception(
            ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS,
            "Cannot create SQL CLUSTER `{}` because a named collection with the same name already exists",
            cluster_name);

    for (const auto & m : members)
        checkSqlClusterMemberNameLocked(m);

    ClusterCatalogDefinition record;
    record.members = members;
    record.secret = cluster_secret;
    record.allow_distributed_ddl_queries = allow_distributed_ddl_queries;
    String create_sql = formatCreateClusterStatement(cluster_name, members, cluster_secret, allow_distributed_ddl_queries);
    clusters_metadata_storage->writeCreateStatement(cluster_name, create_sql, false);
    loaded_sql_clusters[cluster_name] = std::move(record);
    UInt64 version = ++sql_catalog_mutation_counter;
    auto reg_it = cluster_registrations.find(cluster_name);
    if (reg_it == cluster_registrations.end() || reg_it->second.source != ClusterDefinitionSource::RemoteServersConfig)
        cluster_registrations[cluster_name] = ClusterDefinitionRegistration{ClusterDefinitionSource::SqlCatalog, version};
}

bool ClusterFactory::dropCluster(const String & cluster_name, bool if_exists)
{
    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    auto it = loaded_sql_clusters.find(cluster_name);
    if (it == loaded_sql_clusters.end())
    {
        if (!if_exists)
            throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "SQL CLUSTER `{}` does not exist", cluster_name);
        return false;
    }
    auto reg_it = cluster_registrations.find(cluster_name);
    LOG_INFO(log, "Removing SQL CLUSTER catalog definition for `{}` from clusters catalog storage", cluster_name);
    clusters_metadata_storage->remove(cluster_name);
    LOG_INFO(log, "Removed SQL CLUSTER catalog definition for `{}` from clusters catalog storage", cluster_name);
    loaded_sql_clusters.erase(it);
    if (reg_it != cluster_registrations.end() && reg_it->second.source == ClusterDefinitionSource::SqlCatalog)
        cluster_registrations.erase(reg_it);
    return true;
}

bool ClusterFactory::addClusterMembersFromSQL(const ASTAlterClusterQuery & query)
{
    if (query.command != AlterClusterCommand::AddShard)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::addClusterMembersFromSQL expects AddShard");

    if (query.add_shard_members.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ADD SHARD requires at least one member name");

    loadNamedCollectionsIfNeeded();

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (!loaded_sql_clusters.contains(query.cluster_name))
    {
        if (query.if_exists)
            return false;
        throw Exception(
            ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", query.cluster_name);
    }

    ClusterCatalogDefinition record = loaded_sql_clusters.at(query.cluster_name);

    for (const auto & name : query.add_shard_members)
    {
        checkSqlClusterMemberNameLocked(name);
        if (std::find(record.members.begin(), record.members.end(), name) != record.members.end())
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "SQL CLUSTER member `{}` is already listed in SQL CLUSTER `{}`",
                name,
                query.cluster_name);
        }
        record.members.push_back(name);
    }

    String create_sql = formatCreateClusterStatement(
        query.cluster_name, record.members, record.secret, record.allow_distributed_ddl_queries);
    clusters_metadata_storage->writeCreateStatement(query.cluster_name, create_sql, true);
    loaded_sql_clusters[query.cluster_name] = std::move(record);

    UInt64 version = ++sql_catalog_mutation_counter;
    auto reg_it = cluster_registrations.find(query.cluster_name);
    if (reg_it == cluster_registrations.end() || reg_it->second.source != ClusterDefinitionSource::RemoteServersConfig)
        cluster_registrations[query.cluster_name] = ClusterDefinitionRegistration{ClusterDefinitionSource::SqlCatalog, version};
    return true;
}

bool ClusterFactory::dropClusterMembersFromSQL(const ASTAlterClusterQuery & query)
{
    if (query.command != AlterClusterCommand::DropShard)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::dropClusterMembersFromSQL expects DropShard");

    if (query.drop_shard_members.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DROP SHARD requires at least one member name");

    loadNamedCollectionsIfNeeded();

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (!loaded_sql_clusters.contains(query.cluster_name))
    {
        if (query.if_exists)
            return false;
        throw Exception(
            ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", query.cluster_name);
    }

    ClusterCatalogDefinition record = loaded_sql_clusters.at(query.cluster_name);
    auto & mems = record.members;

    for (const auto & name : query.drop_shard_members)
    {
        auto it = std::find(mems.begin(), mems.end(), name);
        if (it == mems.end())
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "SQL CLUSTER member `{}` is not listed in SQL CLUSTER `{}`",
                name,
                query.cluster_name);
        }
        mems.erase(it);
    }

    if (mems.empty())
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cannot DROP all members from SQL CLUSTER `{}`", query.cluster_name);

    String create_sql = formatCreateClusterStatement(
        query.cluster_name, record.members, record.secret, record.allow_distributed_ddl_queries);
    clusters_metadata_storage->writeCreateStatement(query.cluster_name, create_sql, true);
    loaded_sql_clusters[query.cluster_name] = std::move(record);

    UInt64 version = ++sql_catalog_mutation_counter;
    auto reg_it = cluster_registrations.find(query.cluster_name);
    if (reg_it == cluster_registrations.end() || reg_it->second.source != ClusterDefinitionSource::RemoteServersConfig)
        cluster_registrations[query.cluster_name] = ClusterDefinitionRegistration{ClusterDefinitionSource::SqlCatalog, version};
    return true;
}

bool ClusterFactory::replaceClusterMembersFromSQL(const ASTAlterClusterQuery & query)
{
    if (query.command != AlterClusterCommand::ReplaceClusterMembers)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::replaceClusterMembersFromSQL expects ReplaceClusterMembers");

    if (query.member_replace_clauses.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE requires at least one FROM/TO list pair");

    loadNamedCollectionsIfNeeded();

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (!loaded_sql_clusters.contains(query.cluster_name))
    {
        if (query.if_exists)
            return false;
        throw Exception(
            ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", query.cluster_name);
    }

    ClusterCatalogDefinition record = loaded_sql_clusters.at(query.cluster_name);

    std::unordered_map<String, String> repl_map;
    for (const auto & cl : query.member_replace_clauses)
    {
        if (cl.from_members.size() != cl.to_members.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE FROM/TO lists must have equal length");

        for (size_t i = 0; i < cl.from_members.size(); ++i)
        {
            const String & from_name = cl.from_members[i];
            const String & to_name = cl.to_members[i];

            checkSqlClusterMemberNameLocked(to_name);

            auto [it, inserted] = repl_map.emplace(from_name, to_name);
            if (!inserted && it->second != to_name)
            {
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Conflicting REPLACE mappings for member `{}` on SQL CLUSTER `{}`",
                    from_name,
                    query.cluster_name);
            }
        }
    }

    for (const auto & [from_name, to_name] : repl_map)
    {
        (void)to_name;
        if (std::find(record.members.begin(), record.members.end(), from_name) == record.members.end())
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "SQL CLUSTER member `{}` is not listed in SQL CLUSTER `{}`",
                from_name,
                query.cluster_name);
        }
    }

    for (String & m : record.members)
    {
        if (auto it = repl_map.find(m); it != repl_map.end())
            m = it->second;
    }

    std::unordered_set<String> seen;
    for (const auto & m : record.members)
    {
        if (!seen.insert(m).second)
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "Duplicate member `{}` after REPLACE on SQL CLUSTER `{}`",
                m,
                query.cluster_name);
        }
    }

    if (!query.cluster_definition_properties.empty())
    {
        SQLClusterCatalogPropertyValidationDetail::assertNoDuplicatePropertyNames(query.cluster_definition_properties);

        for (const auto & ch : query.cluster_definition_properties)
        {
            if (ch.name == "secret")
            {
                if (ch.value.getType() != Field::Types::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `secret` must be a string");
                record.secret = ch.value.safeGet<String>();
            }
            else if (ch.name == "allow_distributed_ddl_queries")
            {
                if (ch.value.getType() == Field::Types::Bool)
                    record.allow_distributed_ddl_queries = ch.value.safeGet<bool>();
                else
                    record.allow_distributed_ddl_queries = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), ch.value) != 0;
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unknown property `{}` in ALTER CLUSTER ... REPLACE ... MODIFY PROPERTIES (allowed: secret, allow_distributed_ddl_queries)",
                    ch.name);
            }
        }
    }

    String create_sql = formatCreateClusterStatement(
        query.cluster_name, record.members, record.secret, record.allow_distributed_ddl_queries);
    clusters_metadata_storage->writeCreateStatement(query.cluster_name, create_sql, true);
    loaded_sql_clusters[query.cluster_name] = std::move(record);

    UInt64 version = ++sql_catalog_mutation_counter;
    auto reg_it = cluster_registrations.find(query.cluster_name);
    if (reg_it == cluster_registrations.end() || reg_it->second.source != ClusterDefinitionSource::RemoteServersConfig)
        cluster_registrations[query.cluster_name] = ClusterDefinitionRegistration{ClusterDefinitionSource::SqlCatalog, version};
    return true;
}

bool ClusterFactory::updateShardPropertiesFromSQL(const ASTAlterShardQuery & query)
{
    if (query.command != AlterShardCommand::ModifyShardProperties)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::updateShardPropertiesFromSQL expects ModifyShardProperties");

    if (query.shard_definition_properties.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER SHARD ... MODIFY PROPERTIES requires at least one assignment");

    loadNamedCollectionsIfNeeded();

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (!loaded_sql_shards.contains(query.shard_name))
    {
        if (query.if_exists)
            return false;
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", query.shard_name);
    }

    ShardCatalogDefinition record = loaded_sql_shards.at(query.shard_name);

    SQLClusterCatalogPropertyValidationDetail::assertNoDuplicatePropertyNames(query.shard_definition_properties);

    for (const auto & ch : query.shard_definition_properties)
    {
        if (ch.name == "weight")
        {
            record.weight = static_cast<UInt32>(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), ch.value));
            if (record.weight == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `weight` must be greater than zero");
        }
        else if (ch.name == "internal_replication")
            parseShardCatalogInternalReplicationValue(ch.value, record.internal_replication);
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown property `{}` in ALTER SHARD ... MODIFY PROPERTIES (allowed: weight, internal_replication)",
                ch.name);
        }
    }

    String create_sql = formatCreateShardStatement(
        query.shard_name,
        record.replica_collections,
        record.weight,
        record.internal_replication);
    shards_metadata_storage->writeCreateStatement(query.shard_name, create_sql, true);
    loaded_sql_shards[query.shard_name] = std::move(record);
    return true;
}

bool ClusterFactory::addReplicaToShardFromSQL(const ASTAlterShardQuery & query)
{
    if (query.command != AlterShardCommand::AddReplica)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::addReplicaToShardFromSQL expects AddReplica");

    if (query.replica_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ADD REPLICA requires a named collection name");

    loadNamedCollectionsIfNeeded();

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (!loaded_sql_shards.contains(query.shard_name))
    {
        if (query.if_exists)
            return false;
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", query.shard_name);
    }

    if (!namedCollectionExists(query.replica_name))
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Replica named collection `{}` does not exist", query.replica_name);

    ShardCatalogDefinition record = loaded_sql_shards.at(query.shard_name);
    if (std::find(record.replica_collections.begin(), record.replica_collections.end(), query.replica_name)
        != record.replica_collections.end())
    {
        throw Exception(
            ErrorCodes::BAD_CLUSTER_DEFINITION,
            "Replica named collection `{}` is already listed on SQL SHARD `{}`",
            query.replica_name,
            query.shard_name);
    }

    const auto new_collection = getNamedCollection(query.replica_name);
    const auto new_endpoint = tryExtractSqlCatalogReplicaHostPort(*new_collection);
    if (new_endpoint.valid)
    {
        for (const auto & existing_name : record.replica_collections)
        {
            const auto existing_collection = getNamedCollection(existing_name);
            const auto existing_endpoint = tryExtractSqlCatalogReplicaHostPort(*existing_collection);
            if (existing_endpoint.valid && existing_endpoint.host == new_endpoint.host
                && existing_endpoint.port == new_endpoint.port)
            {
                LOG_WARNING(
                    log,
                    "Replica {} points to same endpoint as {} ({}:{})",
                    query.replica_name,
                    existing_name,
                    new_endpoint.host,
                    new_endpoint.port);
                break;
            }
        }
    }

    record.replica_collections.push_back(query.replica_name);

    String create_sql = formatCreateShardStatement(
        query.shard_name,
        record.replica_collections,
        record.weight,
        record.internal_replication);
    shards_metadata_storage->writeCreateStatement(query.shard_name, create_sql, true);
    loaded_sql_shards[query.shard_name] = std::move(record);
    return true;
}

bool ClusterFactory::dropReplicaFromShardFromSQL(const ASTAlterShardQuery & query)
{
    if (query.command != AlterShardCommand::DropReplica)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::dropReplicaFromShardFromSQL expects DropReplica");

    if (query.replica_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DROP REPLICA requires a named collection name");

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (!loaded_sql_shards.contains(query.shard_name))
    {
        if (query.if_exists)
            return false;
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", query.shard_name);
    }

    ShardCatalogDefinition record = loaded_sql_shards.at(query.shard_name);
    auto & reps = record.replica_collections;
    auto it = std::find(reps.begin(), reps.end(), query.replica_name);
    if (it == reps.end())
        throw Exception(
            ErrorCodes::BAD_CLUSTER_DEFINITION,
            "Replica named collection `{}` is not listed on SQL SHARD `{}`",
            query.replica_name,
            query.shard_name);

    if (reps.size() <= 1)
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cannot DROP the last replica from SQL SHARD `{}`", query.shard_name);

    reps.erase(it);

    String create_sql = formatCreateShardStatement(
        query.shard_name,
        record.replica_collections,
        record.weight,
        record.internal_replication);
    shards_metadata_storage->writeCreateStatement(query.shard_name, create_sql, true);
    loaded_sql_shards[query.shard_name] = std::move(record);
    return true;
}

bool ClusterFactory::replaceShardReplicasFromSQL(const ASTAlterShardQuery & query)
{
    if (query.command != AlterShardCommand::ReplaceReplicas)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory::replaceShardReplicasFromSQL expects ReplaceReplicas");

    if (query.replica_replace_clauses.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE requires at least one FROM/TO list pair");

    loadNamedCollectionsIfNeeded();

    std::lock_guard lock(mutex);
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterFactory is not initialized");

    if (!loaded_sql_shards.contains(query.shard_name))
    {
        if (query.if_exists)
            return false;
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", query.shard_name);
    }

    ShardCatalogDefinition record = loaded_sql_shards.at(query.shard_name);

    std::unordered_map<String, String> repl_map;
    for (const auto & cl : query.replica_replace_clauses)
    {
        if (cl.from_collections.size() != cl.to_collections.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE clause list lengths mismatch");

        for (size_t i = 0; i < cl.from_collections.size(); ++i)
        {
            const String & from_name = cl.from_collections[i];
            const String & to_name = cl.to_collections[i];

            if (!namedCollectionExists(to_name))
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Named collection `{}` does not exist (REPLACE ... TO target must exist)",
                    to_name);

            auto [it, inserted] = repl_map.emplace(from_name, to_name);
            if (!inserted && it->second != to_name)
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Conflicting REPLACE mappings for named collection `{}` on SQL SHARD `{}`",
                    from_name,
                    query.shard_name);
        }
    }

    for (const auto & [from_name, to_name] : repl_map)
    {
        (void)to_name;
        if (std::find(record.replica_collections.begin(), record.replica_collections.end(), from_name)
            == record.replica_collections.end())
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "Named collection `{}` is not a replica of SQL SHARD `{}`",
                from_name,
                query.shard_name);
        }
    }

    for (String & coll : record.replica_collections)
    {
        if (auto it = repl_map.find(coll); it != repl_map.end())
            coll = it->second;
    }

    std::unordered_set<String> seen;
    for (const auto & c : record.replica_collections)
    {
        if (!seen.insert(c).second)
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "Duplicate replica `{}` after REPLACE on SQL SHARD `{}`",
                c,
                query.shard_name);
    }

    if (!query.shard_definition_properties.empty())
    {
        SQLClusterCatalogPropertyValidationDetail::assertNoDuplicatePropertyNames(query.shard_definition_properties);

        for (const auto & ch : query.shard_definition_properties)
        {
            if (ch.name == "weight")
            {
                record.weight = static_cast<UInt32>(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), ch.value));
                if (record.weight == 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `weight` must be greater than zero");
            }
            else if (ch.name == "internal_replication")
                parseShardCatalogInternalReplicationValue(ch.value, record.internal_replication);
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unknown property `{}` in ALTER SHARD ... REPLACE ... MODIFY PROPERTIES (allowed: weight, internal_replication)",
                    ch.name);
            }
        }
    }

    String create_sql = formatCreateShardStatement(
        query.shard_name,
        record.replica_collections,
        record.weight,
        record.internal_replication);
    shards_metadata_storage->writeCreateStatement(query.shard_name, create_sql, true);
    loaded_sql_shards[query.shard_name] = std::move(record);
    return true;
}

bool ClusterFactory::hasShard(const String & name) const
{
    std::lock_guard lock(mutex);
    return loaded_sql_shards.contains(name);
}

bool ClusterFactory::hasCluster(const String & name) const
{
    std::lock_guard lock(mutex);
    return loaded_sql_clusters.contains(name);
}

std::vector<String> ClusterFactory::listClusterNames() const
{
    std::lock_guard lock(mutex);
    std::vector<String> out;
    out.reserve(loaded_sql_clusters.size());
    for (const auto & kv : loaded_sql_clusters)
        out.push_back(kv.first);
    return out;
}

std::vector<String> ClusterFactory::listSqlClustersContainingMember(const String & member_name) const
{
    std::lock_guard lock(mutex);
    std::vector<String> out;
    for (const auto & [cname, crec] : loaded_sql_clusters)
    {
        for (const auto & m : crec.members)
        {
            if (m == member_name)
            {
                out.push_back(cname);
                break;
            }
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<SQLShardCatalogTableRow> ClusterFactory::listShardsForSystemTable() const
{
    std::lock_guard lock(mutex);
    std::vector<SQLShardCatalogTableRow> out;
    if (!initialized)
        return out;

    out.reserve(loaded_sql_shards.size());
    for (const auto & [shard_name, rec] : loaded_sql_shards)
    {
        SQLShardCatalogTableRow row;
        row.name = shard_name;
        row.replica_collections = rec.replica_collections;
        row.weight = rec.weight;
        row.internal_replication = rec.internal_replication;

        std::set<String> ref_clusters;
        for (const auto & [cname, crec] : loaded_sql_clusters)
        {
            for (const auto & member : crec.members)
            {
                if (member == shard_name)
                {
                    ref_clusters.insert(cname);
                    break;
                }
            }
        }
        row.referenced_by_clusters.assign(ref_clusters.begin(), ref_clusters.end());
        out.push_back(std::move(row));
    }

    std::sort(out.begin(), out.end(), [](const SQLShardCatalogTableRow & a, const SQLShardCatalogTableRow & b) { return a.name < b.name; });
    return out;
}

String ClusterFactory::getShowCreateShard(const String & name) const
{
    std::lock_guard lock(mutex);
    auto it = loaded_sql_shards.find(name);
    if (it == loaded_sql_shards.end())
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "SQL SHARD `{}` does not exist", name);

    String out = "CREATE SHARD " + backQuoteIfNeed(name) + " REPLICA (";
    for (size_t i = 0; i < it->second.replica_collections.size(); ++i)
    {
        if (i)
            out += ", ";
        out += backQuoteIfNeed(it->second.replica_collections[i]);
    }
    out += ") PROPERTIES (weight = " + std::to_string(it->second.weight) + ", internal_replication = "
        + (it->second.internal_replication ? String{"true"} : String{"false"});
    out += ")";
    return out;
}

String ClusterFactory::getShowCreateCluster(const String & name) const
{
    std::lock_guard lock(mutex);
    auto it = loaded_sql_clusters.find(name);
    if (it == loaded_sql_clusters.end())
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "SQL CLUSTER `{}` does not exist", name);

    return formatCreateClusterStatement(name, it->second.members, it->second.secret, it->second.allow_distributed_ddl_queries);
}

ClusterPtr ClusterFactory::tryMaterializeCluster(const String & cluster_name, ContextPtr context) const
{
    loadNamedCollectionsIfNeeded();

    ClusterCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!initialized)
            return nullptr;
        auto it = loaded_sql_clusters.find(cluster_name);
        if (it == loaded_sql_clusters.end())
            return nullptr;
        record = it->second;
    }

    auto global_context = context->getGlobalContext();
    const auto & settings = global_context->getSettingsRef();
    UInt16 clickhouse_port = global_context->getTCPPort();
    UInt16 default_port = clickhouse_port;

    std::vector<Cluster::ShardInitSpec> specs;
    UInt32 shard_index = 1;
    {
        std::lock_guard lock(mutex);
        for (const auto & member : record.members)
        {
            if (loaded_sql_shards.contains(member))
            {
                const auto & srec = loaded_sql_shards.at(member);
                Cluster::Addresses addresses;
                UInt32 replica_index = 1;
                for (const auto & rep : srec.replica_collections)
                {
                    auto coll = getNamedCollection(rep);
                    addresses.push_back(
                        makeReplicaAddress(rep, *coll, cluster_name, record.secret, shard_index, replica_index, clickhouse_port));
                    ++replica_index;
                }
                specs.push_back(Cluster::ShardInitSpec{std::move(addresses), srec.weight, srec.internal_replication});
            }
            else
            {
                auto coll = getNamedCollection(member);
                specs.push_back(
                    makeWholeShardSpec(member, *coll, cluster_name, record.secret, shard_index, clickhouse_port, default_port));
            }
            ++shard_index;
        }
    }

    return std::make_shared<Cluster>(
        settings, cluster_name, record.secret, std::move(specs), record.allow_distributed_ddl_queries);
}

std::optional<String> ClusterFactory::tryGetMessageIfNamedCollectionReferencedByClusterCatalog(const String & collection_name) const
{
    std::lock_guard lock(mutex);

    // Shard catalog (`CREATE SHARD`): replica endpoints are named collections.
    for (const auto & [sname, srec] : loaded_sql_shards)
    {
        for (const auto & r : srec.replica_collections)
        {
            if (r == collection_name)
                return "named collection is referenced by SQL SHARD `" + sname + "`";
        }
    }

    // Cluster catalog (`CREATE CLUSTER`): a member can be a whole-shard named collection (NC with `replicas` in its body).
    // That member name is not a row in the shard catalog above, so it is not covered by the shard loop.
    for (const auto & [cname, crec] : loaded_sql_clusters)
    {
        for (const auto & m : crec.members)
        {
            if (m == collection_name && !loaded_sql_shards.contains(m))
                return "named collection is referenced as whole shard by SQL CLUSTER `" + cname + "`";
        }
    }
    return std::nullopt;
}

std::optional<ClusterDefinitionRegistration> ClusterFactory::getClusterRegistration(const String & cluster_name) const
{
    std::lock_guard lock(mutex);
    auto it = cluster_registrations.find(cluster_name);
    if (it == cluster_registrations.end())
        return std::nullopt;
    return it->second;
}

void ClusterFactory::loadNamedCollectionsIfNeeded()
{
    NamedCollectionFactory::instance().loadIfNot();
}

bool ClusterFactory::namedCollectionExists(const String & name)
{
    return NamedCollectionFactory::instance().exists(name);
}

NamedCollectionPtr ClusterFactory::getNamedCollection(const String & name)
{
    return NamedCollectionFactory::instance().get(name);
}

}
