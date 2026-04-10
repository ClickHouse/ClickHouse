#include <Common/Clusters/ClusterFactory.h>

#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/quoteString.h>
#include <Common/parseAddress.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Core/BackgroundSchedulePool.h>
#include <base/sleep.h>
#include <Parsers/ASTCreateClusterQuery.h>
#include <Parsers/ASTCreateShardQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/string/trim.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <set>

namespace DB
{

namespace ErrorCodes
{
    extern const int SHARD_ALREADY_EXISTS;
    extern const int SHARD_DOESNT_EXIST;
    extern const int CLUSTER_DEFINITION_ALREADY_EXISTS;
    extern const int CLUSTER_DEFINITION_DOESNT_EXIST;
    extern const int BAD_CLUSTER_DEFINITION;
    extern const int CLUSTER_DEFINITION_NAME_AMBIGUOUS;
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
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
    const String & shard_name, const std::vector<String> & replica_collections, UInt32 weight, bool internal_replication)
{
    ASTCreateShardQuery ast;
    ast.shard_name = shard_name;
    ast.replicas = replica_collections;
    ast.weight = weight;
    ast.internal_replication = internal_replication;
    ast.if_not_exists = false;
    return ast.formatWithSecretsOneLine();
}

String formatCreateClusterStatement(const String & cluster_name, const std::vector<String> & members)
{
    ASTCreateClusterQuery ast;
    ast.cluster_name = cluster_name;
    ast.members = members;
    ast.if_not_exists = false;
    return ast.formatWithSecretsOneLine();
}

UInt64 getUIntField(const NamedCollection & coll, const String & key, UInt64 default_value)
{
    if (!coll.has(key))
        return default_value;
    try
    {
        return coll.get<UInt64>(key);
    }
    catch (...)
    {
        return parse<UInt64>(coll.get<String>(key));
    }
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
    addr.port = static_cast<UInt16>(getUIntField(coll, NC_PORT, NC_NUM_DEFAULT_OFF));
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
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
            continue;
        }
    }

    LOG_TRACE(log, "SQL shard/cluster catalog background update thread finished");
}

void ClusterFactory::createShard(
    const String & shard_name, const std::vector<String> & replica_collections, UInt32 weight, bool internal_replication)
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
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Cannot drop SQL SHARD `{}` because SQL CLUSTER `{}` references it",
                    shard_name,
                    cname);
        }
    }

    shards_metadata_storage->remove(shard_name);
    loaded_sql_shards.erase(it);
}

void ClusterFactory::createCluster(const String & cluster_name, const std::vector<String> & members)
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
    {
        if (loaded_sql_shards.contains(m))
            continue;
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

    ClusterCatalogDefinition record;
    record.members = members;
    String create_sql = formatCreateClusterStatement(cluster_name, members);
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
    clusters_metadata_storage->remove(cluster_name);
    loaded_sql_clusters.erase(it);
    if (reg_it != cluster_registrations.end() && reg_it->second.source == ClusterDefinitionSource::SqlCatalog)
        cluster_registrations.erase(reg_it);
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

    String out = "CREATE SHARD " + backQuoteIfNeed(name) + " (";
    for (size_t i = 0; i < it->second.replica_collections.size(); ++i)
    {
        if (i)
            out += ", ";
        out += backQuoteIfNeed(it->second.replica_collections[i]);
    }
    out += ") SETTINGS weight = " + std::to_string(it->second.weight) + ", internal_replication = "
        + (it->second.internal_replication ? String{"true"} : String{"false"});
    return out;
}

String ClusterFactory::getShowCreateCluster(const String & name) const
{
    std::lock_guard lock(mutex);
    auto it = loaded_sql_clusters.find(name);
    if (it == loaded_sql_clusters.end())
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "SQL CLUSTER `{}` does not exist", name);

    String out = "CREATE CLUSTER " + backQuoteIfNeed(name) + " (";
    for (size_t i = 0; i < it->second.members.size(); ++i)
    {
        if (i)
            out += ", ";
        out += backQuoteIfNeed(it->second.members[i]);
    }
    out += ")";
    return out;
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
                        makeReplicaAddress(rep, *coll, cluster_name, EMPTY_STRING, shard_index, replica_index, clickhouse_port));
                    ++replica_index;
                }
                specs.push_back(Cluster::ShardInitSpec{std::move(addresses), srec.weight, srec.internal_replication});
            }
            else
            {
                auto coll = getNamedCollection(member);
                specs.push_back(
                    makeWholeShardSpec(member, *coll, cluster_name, EMPTY_STRING, shard_index, clickhouse_port, default_port));
            }
            ++shard_index;
        }
    }

    return std::make_shared<Cluster>(settings, cluster_name, /* cluster_secret */ EMPTY_STRING, std::move(specs));
}

std::optional<String> ClusterFactory::namedCollectionDropBlockReason(const String & collection_name) const
{
    std::lock_guard lock(mutex);
    for (const auto & [sname, srec] : loaded_sql_shards)
    {
        for (const auto & r : srec.replica_collections)
        {
            if (r == collection_name)
                return "named collection is referenced by SQL SHARD `" + sname + "`";
        }
    }
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
