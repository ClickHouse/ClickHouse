#include <Common/Clusters/ClusterMetadataManager.h>

#include <Common/Clusters/ClusterFactory.h>
#include <Common/Clusters/PropertyValidation.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/Field.h>
#include <Core/ServerUUID.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTAlterClusterQuery.h>
#include <Parsers/ASTAlterShardQuery.h>
#include <Parsers/ASTCreateClusterCatalogQuery.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BAD_CLUSTER_DEFINITION;
    extern const int CLUSTER_DEFINITION_ALREADY_EXISTS;
    extern const int CLUSTER_DEFINITION_DOESNT_EXIST;
    extern const int CLUSTER_DEFINITION_NAME_AMBIGUOUS;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int SHARD_ALREADY_EXISTS;
    extern const int SHARD_DOESNT_EXIST;
    extern const int SHARD_IS_REFERENCED;
}

namespace
{

const String NC_DEFAULT_USER{"default"};

String formatCreateShardStatement(
    const String & shard_name,
    const std::vector<String> & endpoint_names,
    UInt32 weight,
    bool internal_replication)
{
    ASTCreateClusterCatalogQuery ast;
    ast.kind = ASTCreateClusterCatalogQuery::Kind::Shard;
    ast.name = shard_name;
    ast.members = endpoint_names;
    ast.properties.clear();
    ast.properties.push_back(SettingChange{"weight", Field{UInt64{weight}}});
    ast.properties.push_back(SettingChange{"internal_replication", Field{internal_replication}});
    ast.if_not_exists = false;
    return ast.formatWithSecretsOneLine();
}

String formatCreateClusterStatement(
    const String & cluster_name,
    const std::vector<String> & members,
    const String & cluster_secret,
    bool allow_distributed_ddl_queries)
{
    ASTCreateClusterCatalogQuery ast;
    ast.kind = ASTCreateClusterCatalogQuery::Kind::Cluster;
    ast.name = cluster_name;
    ast.members = members;
    if (!cluster_secret.empty())
        ast.properties.push_back(SettingChange{"secret", Field{cluster_secret}});
    ast.properties.push_back(SettingChange{"allow_distributed_ddl_queries", Field{allow_distributed_ddl_queries}});
    ast.if_not_exists = false;
    return ast.formatWithSecretsOneLine();
}

bool parseBoolProperty(const Field & value)
{
    if (value.getType() == Field::Types::Bool)
        return value.safeGet<bool>();
    return applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value) != 0;
}

String parseStringProperty(const Field & value, std::string_view property_name)
{
    if (value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` in endpoint-level PROPERTIES must be a string", property_name);
    return value.safeGet<String>();
}

void applyEndpointPropertiesPatch(EndpointCatalogDefinition & definition, const SettingsChanges & properties)
{
    using namespace SQLClusterCatalog;
    PropertyValidation::Replica::validateKeys(properties);

    for (const auto & change : properties)
    {
        if (change.name == "host")
            definition.host = parseStringProperty(change.value, change.name);
        else if (change.name == "port")
        {
            definition.port = PropertyValidation::Replica::narrowPortToUInt16(
                PropertyValidation::Detail::parseUnsignedIntegerPropertyValue(change.value, "port"),
                "Property `port`");
        }
        else if (change.name == "user")
            definition.user = parseStringProperty(change.value, change.name);
        else if (change.name == "password")
            definition.password = parseStringProperty(change.value, change.name);
        else if (change.name == "default_database")
            definition.default_database = parseStringProperty(change.value, change.name);
        else if (change.name == "bind_host")
            definition.bind_host = parseStringProperty(change.value, change.name);
        else if (change.name == "secure")
            definition.secure = parseBoolProperty(change.value);
        else if (change.name == "compression")
            definition.compression = parseBoolProperty(change.value);
        else if (change.name == "priority")
        {
            definition.priority = PropertyValidation::Replica::narrowPriorityToInt64(
                PropertyValidation::Detail::parseUnsignedIntegerPropertyValue(change.value, "priority"),
                "Property `priority`");
        }
    }

    if (definition.host.empty() || !PropertyValidation::Detail::isValidHost(definition.host))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Endpoint requires valid non-empty `host`, got `{}`",
            definition.host);
    }

    if (!definition.port)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Endpoint requires positive `port`");
}

Cluster::Address makeEndpointAddress(
    const EndpointCatalogDefinition & endpoint,
    const String & cluster_name,
    const String & cluster_secret,
    UInt32 shard_index,
    UInt32 replica_index,
    UInt16 clickhouse_port)
{
    Cluster::Address addr;
    addr.host_name = endpoint.host;
    addr.port = endpoint.port;
    if (!addr.port)
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint requires positive `port`");

    addr.user = endpoint.user;
    addr.password = endpoint.password;
    addr.default_database = endpoint.default_database;
    addr.bind_host = endpoint.bind_host;
    addr.priority = Priority{endpoint.priority};
    addr.secure = endpoint.secure ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    addr.shard_index = shard_index;
    addr.replica_index = replica_index;
    addr.cluster = cluster_name;
    addr.cluster_secret = cluster_secret;
    addr.user_specified = endpoint.user != NC_DEFAULT_USER;
    addr.recomputeIsLocal(clickhouse_port);
    addr.compression = (endpoint.compression && !addr.is_local)
        ? Protocol::Compression::Enable
        : Protocol::Compression::Disable;
    return addr;
}

String joinKeeperPath(String root_path, const String & child)
{
    if (root_path.empty())
        return "/" + child;

    if (root_path.front() != '/')
        root_path = "/" + root_path;

    while (root_path.size() > 1 && root_path.ends_with('/'))
        root_path.pop_back();

    if (child.empty())
        return root_path;

    return (fs::path(root_path) / child).string();
}

void validateKeeperChildName(const String & name, std::string_view setting_name)
{
    if (name.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`{}` cannot be empty", setting_name);

    if (name.find('/') != String::npos)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`{}` cannot contain '/': `{}`", setting_name, name);
}

bool isReplicaGroupKey(const String & key)
{
    return key == "replica_group" || key.starts_with("replica_group[");
}

template <typename Map>
std::vector<String> listMapKeys(const Map & map)
{
    std::vector<String> result;
    result.reserve(map.size());
    for (const auto & [name, _] : map)
        result.push_back(name);
    return result;
}

}

ClusterMetadataManager & ClusterMetadataManager::instance()
{
    static ClusterMetadataManager manager;
    return manager;
}

ClusterMetadataManager::~ClusterMetadataManager()
{
    shutdown();
}

void ClusterMetadataManager::initialize()
{
    String initialized_replica_group;

    if (initialized)
        return;

    context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::initialize called before global context exists");

    config = parseConfig(context->getConfigRef());
    if (config.enabled)
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterMetadataManager::initialize");
        auto zookeeper = context->getDefaultOrAuxiliaryZooKeeper(config.keeper_name);
        storage = std::make_shared<ClusterMetadataStorage>(
            zookeeper,
            config.local_root,
            config.encrypted,
            config.encryption_key_hex,
            config.encryption_algorithm);
        storage->initLayout();
        reloadSnapshotUnlocked();

        ddl_worker = std::make_shared<ClusterMetadataDDLWorker>(
            context,
            storage,
            toString(ServerUUID::get()),
            config.keeper_name,
            [this]
            {
                const String digest = reloadSnapshot();
                publishSnapshotToClusterFactory();
                return digest;
            },
            [this](const ClusterMetadataMutation & mutation)
            {
                return applyMutation(mutation);
            });
        initialized_replica_group = config.replica_group;

        if (!config.imports.empty())
        {
            importer = std::make_shared<ClusterMetadataImporter>(
                context,
                config.keeper_name,
                config.root_path,
                config.encrypted,
                config.encryption_key_hex,
                config.encryption_algorithm,
                config.imports,
                [this] { publishSnapshotToClusterFactory(); });
        }
    }
    else
    {
        LOG_INFO(log, "ClusterMetadataManager initialized without `{}` configuration", CONFIG_PREFIX);
    }

    initialized = true;

    if (ddl_worker)
    {
        publishSnapshotToClusterFactory();
        ddl_worker->startup();
        if (importer)
            importer->startup();

        LOG_INFO(log, "ClusterMetadataManager initialized for replica group `{}`", initialized_replica_group);
    }
}

void ClusterMetadataManager::shutdown()
{
    ClusterMetadataImporterPtr importer_to_shutdown;
    {
        std::lock_guard lock(mutex);
        importer_to_shutdown = std::move(importer);
    }
    /// Stop the importer before taking the manager mutex for the rest of shutdown: the importer task
    /// can call back into `publishSnapshotToClusterFactory`, which takes this mutex.
    if (importer_to_shutdown)
        importer_to_shutdown->shutdown();

    std::lock_guard lock(mutex);
    if (ddl_worker)
        ddl_worker->shutdown();
    ddl_worker.reset();
    if (initialized)
        ClusterFactory::instance().replaceSQLCatalogClusters({});
    storage.reset();
    snapshot = {};
    snapshot_version = 0;
    config = {};
    context = nullptr;
    initialized = false;
}

ClusterMetadataConfig ClusterMetadataManager::parseConfig(
    const Poco::Util::AbstractConfiguration & config,
    std::string_view config_prefix_)
{
    const String config_prefix(config_prefix_);

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    if (keys.empty())
        return {};

    size_t replica_group_keys = 0;
    for (const auto & key : keys)
    {
        if (isReplicaGroupKey(key))
            ++replica_group_keys;
    }

    if (replica_group_keys > 1)
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "`{}.replica_group` can be specified only once",
            config_prefix);

    ClusterMetadataConfig result;
    result.enabled = true;
    result.keeper_name = config.getString(config_prefix + ".keeper", String(zkutil::DEFAULT_ZOOKEEPER_NAME));
    result.root_path = config.getString(config_prefix + ".path", "");
    result.encrypted = config.getBool(config_prefix + ".encrypted", false);
    if (result.encrypted)
    {
        result.encryption_key_hex = config.getRawString(config_prefix + ".key_hex", "");
        result.encryption_algorithm = config.getString(config_prefix + ".algorithm", "aes_128_ctr");
    }
    result.replica_group = config.getString(config_prefix + ".replica_group", String(DEFAULT_REPLICA_GROUP));

    if (result.root_path.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`{}.path` cannot be empty", config_prefix);

    validateKeeperChildName(result.replica_group, config_prefix + ".replica_group");

    Poco::Util::AbstractConfiguration::Keys import_keys;
    config.keys(config_prefix + ".imports", import_keys);

    std::unordered_set<String> seen_imports;
    for (const auto & import_key : import_keys)
    {
        if (!isReplicaGroupKey(import_key))
            continue;

        const auto imported_group = config.getString(config_prefix + ".imports." + import_key);
        validateKeeperChildName(imported_group, config_prefix + ".imports." + import_key);

        if (imported_group == result.replica_group)
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "`{}.imports` cannot contain the local replica group `{}`",
                config_prefix,
                imported_group);

        if (seen_imports.insert(imported_group).second)
            result.imports.push_back(imported_group);
    }

    result.root_path = joinKeeperPath(result.root_path, "");
    result.local_root = joinKeeperPath(result.root_path, result.replica_group);

    return result;
}

void ClusterMetadataManager::reloadSnapshotUnlocked()
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataManager::reloadSnapshot");
    snapshot = storage->readSnapshot();
    for (auto & [shard_name, shard] : snapshot.shards)
    {
        if (shard.name.empty())
            shard.name = shard_name;
        resolveEndpointsForShard(shard);
    }
    ++snapshot_version;
}

String ClusterMetadataManager::reloadSnapshot()
{
    if (!isEnabled())
        throwIfDisabled();

    String digest;
    std::lock_guard lock(mutex);
    if (!storage)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Cluster metadata storage is not initialized");
    reloadSnapshotUnlocked();
    digest = snapshot.digest;
    return digest;
}

bool ClusterMetadataManager::hasShard(const String & shard_name) const
{
    std::lock_guard lock(mutex);
    return snapshot.shards.contains(shard_name);
}

std::optional<EndpointCatalogDefinition> ClusterMetadataManager::tryGetEndpoint(const String & endpoint_name) const
{
    if (!isEnabled())
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.endpoints.find(endpoint_name);
    if (it == snapshot.endpoints.end())
        return std::nullopt;
    return it->second;
}

std::optional<ShardCatalogDefinition> ClusterMetadataManager::tryGetShard(const String & shard_name) const
{
    if (!isEnabled())
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.shards.find(shard_name);
    if (it == snapshot.shards.end())
        return std::nullopt;
    return it->second;
}

std::optional<ClusterCatalogDefinition> ClusterMetadataManager::tryGetCluster(const String & cluster_name) const
{
    if (!isEnabled())
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.clusters.find(cluster_name);
    if (it == snapshot.clusters.end())
        return std::nullopt;
    return it->second;
}

std::vector<String> ClusterMetadataManager::listEndpointNames() const
{
    if (!isEnabled())
        throwIfDisabled();
    std::lock_guard lock(mutex);
    return listMapKeys(snapshot.endpoints);
}

std::vector<String> ClusterMetadataManager::listShardNames() const
{
    if (!isEnabled())
        throwIfDisabled();
    std::lock_guard lock(mutex);
    return listMapKeys(snapshot.shards);
}

std::vector<String> ClusterMetadataManager::listClusterNames() const
{
    if (!isEnabled())
        throwIfDisabled();
    std::lock_guard lock(mutex);
    return listMapKeys(snapshot.clusters);
}

bool ClusterMetadataManager::isEnabled() const
{
    std::lock_guard lock(mutex);
    return config.enabled;
}

void ClusterMetadataManager::throwIfDisabled() const
{
    throw Exception(
        ErrorCodes::INVALID_CONFIG_PARAMETER,
        "SQL-managed cluster metadata requires `{}` to be configured in the server config",
        CONFIG_PREFIX);
}

void ClusterMetadataManager::commitMutation(const ClusterMetadataMutation & mutation)
{
    ClusterMetadataDDLWorkerPtr worker;
    {
        std::lock_guard lock(mutex);
        worker = ddl_worker;
    }

    if (!worker)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster metadata DDL worker is not initialized");

    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataManager::commitMutation");
    worker->enqueueMutationAndWait(mutation);
}

String ClusterMetadataManager::applyMutation(const ClusterMetadataMutation & mutation)
{
    if (!isEnabled())
        throwIfDisabled();

    String digest;
    {
        std::lock_guard lock(mutex);
        if (!storage)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Cluster metadata storage is not initialized");
        applyMutationUnlocked(mutation);
        digest = snapshot.digest;
    }
    publishSnapshotToClusterFactory();
    return digest;
}

void ClusterMetadataManager::applyMutationUnlocked(const ClusterMetadataMutation & mutation)
{
    switch (mutation.type)
    {
        case ClusterMetadataMutation::Type::CreateEndpoint:
        case ClusterMetadataMutation::Type::AlterEndpoint:
        {
            snapshot.endpoints[mutation.name] = EndpointCatalogDefinition::deserialize(mutation.definition_data);
            for (auto & shard_entry : snapshot.shards)
            {
                auto & shard = shard_entry.second;
                if (std::find(shard.endpoint_names.begin(), shard.endpoint_names.end(), mutation.name) != shard.endpoint_names.end())
                    resolveEndpointsForShard(shard);
            }
            break;
        }
        case ClusterMetadataMutation::Type::DropEndpoint:
            snapshot.endpoints.erase(mutation.name);
            break;
        case ClusterMetadataMutation::Type::CreateShard:
        case ClusterMetadataMutation::Type::AlterShard:
        {
            auto shard = ShardCatalogDefinition::deserialize(mutation.definition_data);
            if (shard.name.empty())
                shard.name = mutation.name;
            resolveEndpointsForShard(shard);
            snapshot.shards[mutation.name] = std::move(shard);
            break;
        }
        case ClusterMetadataMutation::Type::DropShard:
            snapshot.shards.erase(mutation.name);
            break;
        case ClusterMetadataMutation::Type::CreateCluster:
        case ClusterMetadataMutation::Type::AlterCluster:
            snapshot.clusters[mutation.name] = ClusterCatalogDefinition::deserialize(mutation.definition_data);
            break;
        case ClusterMetadataMutation::Type::DropCluster:
            snapshot.clusters.erase(mutation.name);
            break;
    }

    snapshot.digest = storage->calculateDigest(snapshot);
    ++snapshot_version;
}

void ClusterMetadataManager::commitAlterShard(const ShardCatalogDefinition & definition)
{
    if (definition.name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ShardCatalogDefinition requires non-empty name");

    commitMutation(ClusterMetadataMutation::alterShard(definition));
}

void ClusterMetadataManager::commitAlterCluster(const String & cluster_name, const ClusterCatalogDefinition & definition)
{
    commitMutation(ClusterMetadataMutation::alterCluster(cluster_name, definition));
}

void ClusterMetadataManager::materializeSnapshotClusters(
    const ClusterMetadataStorage::Snapshot & source_snapshot,
    ContextPtr query_context,
    std::map<String, ClusterPtr> & out) const
{
    for (const auto & [cluster_name, cluster_record] : source_snapshot.clusters)
    {
        if (auto cluster = materializeClusterFromSnapshot(cluster_name, cluster_record, source_snapshot, query_context))
            out.emplace(cluster_name, cluster);
    }
}

void ClusterMetadataManager::publishSnapshotToClusterFactory() const
{
    ContextPtr local_context;
    ClusterMetadataStorage::Snapshot local_snapshot;
    UInt64 version_snapshot = 0;
    ClusterMetadataImporterPtr local_importer;
    {
        std::lock_guard lock(mutex);
        local_context = context;
        local_snapshot = snapshot;
        version_snapshot = snapshot_version;
        local_importer = importer;
    }

    const auto imported_snapshots = local_importer ? local_importer->getLoadedSnapshots() : std::vector<ClusterMetadataImporter::ImportedSnapshot>{};

    if (!local_context)
        local_context = Context::getGlobalContextInstance();
    if (!local_context)
        return;

    std::map<String, ClusterPtr> materialized;
    materializeSnapshotClusters(local_snapshot, local_context, materialized);

    /// Imported (read-only) clusters fill in names not already defined locally; on a name collision the
    /// local replica group always wins, and among imports the first configured group wins.
    for (const auto & imported_snapshot : imported_snapshots)
    {
        std::map<String, ClusterPtr> imported_clusters;
        try
        {
            materializeSnapshotClusters(imported_snapshot.snapshot, local_context, imported_clusters);
        }
        catch (...)
        {
            tryLogCurrentException(
                log, fmt::format("Failed to materialize imported clusters from replica group `{}`", imported_snapshot.replica_group));
            continue;
        }

        for (auto & [name, cluster] : imported_clusters)
        {
            if (!materialized.emplace(name, cluster).second)
                LOG_WARNING(
                    log,
                    "Imported SQL CLUSTER `{}` from replica group `{}` is shadowed by an already-visible cluster of the same name",
                    name,
                    imported_snapshot.replica_group);
        }
    }

    for (auto & [_, cluster] : materialized)
        cluster->setDefinitionMetadata(ClusterDefinitionSource::SQLCatalog, version_snapshot);

    ClusterFactory::instance().replaceSQLCatalogClusters(materialized);
}

ClusterPtr ClusterMetadataManager::materializeClusterFromSnapshot(
    const String & cluster_name,
    const ClusterCatalogDefinition & record,
    const ClusterMetadataStorage::Snapshot & local_snapshot,
    ContextPtr query_context) const
{
    if (!query_context)
        return nullptr;

    auto global_context = query_context->getGlobalContext();
    const auto & settings = global_context->getSettingsRef();
    const UInt16 clickhouse_port = global_context->getTCPPort();

    std::vector<Cluster::ShardInitSpec> specs;
    UInt32 shard_index = 1;
    for (const auto & member : record.members)
    {
        const auto shard_it = local_snapshot.shards.find(member);
        if (shard_it == local_snapshot.shards.end())
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "SQL CLUSTER `{}` member shard `{}` does not exist",
                cluster_name,
                member);
        }

        const auto & shard_record = shard_it->second;
        Cluster::Addresses addresses;
        UInt32 replica_index = 1;
        for (const auto & endpoint : shard_record.endpoints)
        {
            addresses.push_back(
                makeEndpointAddress(endpoint, cluster_name, record.secret, shard_index, replica_index, clickhouse_port));
            ++replica_index;
        }
        if (addresses.empty())
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "SQL SHARD `{}` referenced by cluster `{}` has no endpoints",
                member,
                cluster_name);
        }
        specs.push_back(Cluster::ShardInitSpec{std::move(addresses), shard_record.weight, shard_record.internal_replication});
        ++shard_index;
    }

    return std::make_shared<Cluster>(
        settings, cluster_name, record.secret, std::move(specs), record.allow_distributed_ddl_queries);
}

ClusterPtr ClusterMetadataManager::materializeCluster(const String & cluster_name, ContextPtr query_context) const
{
    if (!query_context)
        return nullptr;

    ClusterCatalogDefinition record;
    ClusterMetadataStorage::Snapshot local_snapshot;
    {
        std::lock_guard lock(mutex);
        const auto it = snapshot.clusters.find(cluster_name);
        if (it == snapshot.clusters.end())
            return nullptr;
        record = it->second;
        local_snapshot = snapshot;
    }

    return materializeClusterFromSnapshot(cluster_name, record, local_snapshot, query_context);
}

String ClusterMetadataManager::getShowCreateShard(const String & shard_name) const
{
    if (!isEnabled())
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.shards.find(shard_name);
    if (it == snapshot.shards.end())
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "SQL SHARD `{}` does not exist", shard_name);

    return formatCreateShardStatement(it->second.name, it->second.endpoint_names, it->second.weight, it->second.internal_replication);
}

String ClusterMetadataManager::getShowCreateCluster(const String & cluster_name) const
{
    if (!isEnabled())
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.clusters.find(cluster_name);
    if (it == snapshot.clusters.end())
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "SQL CLUSTER `{}` does not exist", cluster_name);

    return formatCreateClusterStatement(
        cluster_name, it->second.members, it->second.secret, it->second.allow_distributed_ddl_queries);
}

std::vector<ShardCatalogDefinition> ClusterMetadataManager::listShardsForSystemTable() const
{
    std::lock_guard lock(mutex);

    std::vector<ShardCatalogDefinition> out;
    out.reserve(snapshot.shards.size());
    for (const auto & [shard_name, shard_record] : snapshot.shards)
    {
        ShardCatalogDefinition shard = shard_record;
        if (shard.name.empty())
            shard.name = shard_name;

        std::set<String> ref_clusters;
        for (const auto & [cluster_name, cluster_record] : snapshot.clusters)
        {
            for (const auto & member : cluster_record.members)
            {
                if (member == shard_name)
                {
                    ref_clusters.insert(cluster_name);
                    break;
                }
            }
        }
        shard.referenced_by_clusters.assign(ref_clusters.begin(), ref_clusters.end());
        out.push_back(std::move(shard));
    }

    std::sort(out.begin(), out.end(), [](const ShardCatalogDefinition & a, const ShardCatalogDefinition & b) { return a.name < b.name; });
    return out;
}

std::vector<EndpointCatalogSystemTableRow> ClusterMetadataManager::listEndpointsForSystemTable() const
{
    std::lock_guard lock(mutex);

    std::unordered_map<String, std::set<String>> endpoint_to_shards;
    for (const auto & [shard_name, shard_record] : snapshot.shards)
    {
        for (const auto & endpoint_name : shard_record.endpoint_names)
            endpoint_to_shards[endpoint_name].insert(shard_name);
    }

    std::vector<EndpointCatalogSystemTableRow> out;
    out.reserve(snapshot.endpoints.size());
    for (const auto & [endpoint_name, endpoint_record] : snapshot.endpoints)
    {
        EndpointCatalogSystemTableRow row;
        row.name = endpoint_name;
        row.endpoint = endpoint_record;
        if (const auto it = endpoint_to_shards.find(endpoint_name); it != endpoint_to_shards.end())
            row.bound_shards.assign(it->second.begin(), it->second.end());
        out.emplace_back(std::move(row));
    }

    std::sort(out.begin(), out.end(), [](const auto & lhs, const auto & rhs) { return lhs.name < rhs.name; });
    return out;
}

std::vector<String> ClusterMetadataManager::listSQLClustersContainingMember(const String & member_name) const
{
    std::lock_guard lock(mutex);

    std::vector<String> out;
    for (const auto & [cluster_name, cluster_record] : snapshot.clusters)
    {
        for (const auto & member : cluster_record.members)
        {
            if (member == member_name)
            {
                out.push_back(cluster_name);
                break;
            }
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

void ClusterMetadataManager::assertShardNameAvailable(const String & shard_name) const
{
    if (snapshot.shards.contains(shard_name))
        return;
    if (snapshot.clusters.contains(shard_name))
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL CLUSTER", shard_name);
    if (snapshot.endpoints.contains(shard_name))
        throw Exception(
            ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS,
            "Cannot create SQL SHARD `{}` because an endpoint with the same name already exists",
            shard_name);
}

void ClusterMetadataManager::assertClusterNameAvailable(const String & cluster_name) const
{
    if (snapshot.clusters.contains(cluster_name))
        return;
    if (snapshot.shards.contains(cluster_name))
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL SHARD", cluster_name);
    if (snapshot.endpoints.contains(cluster_name))
        throw Exception(
            ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS,
            "Cannot create SQL CLUSTER `{}` because an endpoint with the same name already exists",
            cluster_name);
}

void ClusterMetadataManager::assertEndpointNameAvailable(const String & endpoint_name) const
{
    if (snapshot.endpoints.contains(endpoint_name))
        return;
    if (snapshot.shards.contains(endpoint_name))
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL SHARD", endpoint_name);
    if (snapshot.clusters.contains(endpoint_name))
        throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL CLUSTER", endpoint_name);
}

void ClusterMetadataManager::validateClusterMemberShardExists(const String & shard_name) const
{
    if (!snapshot.shards.contains(shard_name))
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cluster metadata shard `{}` does not exist", shard_name);
}

void ClusterMetadataManager::validateClusterTotalShardWeight(
    const String & cluster_name,
    const std::vector<String> & members,
    const ShardCatalogDefinition * shard_override) const
{
    UInt64 total_weight = 0;
    for (const auto & member : members)
    {
        UInt32 weight = 0;
        if (shard_override && shard_override->name == member)
        {
            weight = shard_override->weight;
        }
        else
        {
            const auto shard_it = snapshot.shards.find(member);
            if (shard_it == snapshot.shards.end())
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cluster metadata shard `{}` does not exist", member);
            weight = shard_it->second.weight;
        }

        total_weight += weight;
        if (total_weight > Cluster::MAX_TOTAL_SHARD_WEIGHT)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "SQL CLUSTER `{}` total shard weight must not exceed {}, got at least {}",
                cluster_name,
                Cluster::MAX_TOTAL_SHARD_WEIGHT,
                total_weight);
        }
    }
}

std::vector<String> ClusterMetadataManager::listClustersContainingShard(const String & shard_name) const
{
    std::vector<String> result;
    for (const auto & [cluster_name, cluster] : snapshot.clusters)
    {
        if (std::find(cluster.members.begin(), cluster.members.end(), shard_name) != cluster.members.end())
            result.push_back(cluster_name);
    }
    std::sort(result.begin(), result.end());
    return result;
}

bool ClusterMetadataManager::endpointsMatch(const EndpointCatalogDefinition & lhs, const EndpointCatalogDefinition & rhs)
{
    return lhs.host == rhs.host && lhs.port == rhs.port;
}

void ClusterMetadataManager::resolveShardEndpoints(
    ShardCatalogDefinition & shard,
    const std::unordered_map<String, EndpointCatalogDefinition> & endpoints)
{
    shard.endpoints.clear();
    shard.endpoints.reserve(shard.endpoint_names.size());
    for (const auto & endpoint_name : shard.endpoint_names)
    {
        const auto it = endpoints.find(endpoint_name);
        if (it == endpoints.end())
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", endpoint_name);
        shard.endpoints.push_back(it->second);
    }
}

void ClusterMetadataManager::resolveEndpointsForShard(ShardCatalogDefinition & shard) const
{
    resolveShardEndpoints(shard, snapshot.endpoints);
}

ShardCatalogDefinition ClusterMetadataManager::buildShardDefinition(
    const String & shard_name,
    const std::vector<String> & endpoint_names,
    UInt32 weight,
    bool internal_replication) const
{
    if (endpoint_names.empty())
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "CREATE SHARD requires at least one endpoint");

    for (const auto & endpoint_name : endpoint_names)
    {
        if (!snapshot.endpoints.contains(endpoint_name))
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", endpoint_name);
    }

    ShardCatalogDefinition record;
    record.name = shard_name;
    record.endpoint_names = endpoint_names;
    record.weight = weight;
    record.internal_replication = internal_replication;
    resolveEndpointsForShard(record);
    return record;
}

bool ClusterMetadataManager::createEndpoint(
    const String & endpoint_name,
    const EndpointCatalogDefinition & definition,
    bool if_not_exists)
{
    if (!isEnabled())
        throwIfDisabled();

    {
        std::lock_guard lock(mutex);
        assertEndpointNameAvailable(endpoint_name);

        if (snapshot.endpoints.contains(endpoint_name))
        {
            if (if_not_exists)
                return false;
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` already exists", endpoint_name);
        }
    }

    commitMutation(ClusterMetadataMutation::createEndpoint(endpoint_name, definition));
    return true;
}

bool ClusterMetadataManager::dropEndpoint(const String & endpoint_name, bool if_exists)
{
    if (!isEnabled())
        throwIfDisabled();

    {
        std::lock_guard lock(mutex);
        if (!snapshot.endpoints.contains(endpoint_name))
        {
            if (if_exists)
                return false;
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", endpoint_name);
        }

        for (const auto & [shard_name, shard] : snapshot.shards)
        {
            if (std::find(shard.endpoint_names.begin(), shard.endpoint_names.end(), endpoint_name) != shard.endpoint_names.end())
            {
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Cannot drop cluster metadata endpoint `{}` because shard `{}` references it",
                    endpoint_name,
                    shard_name);
            }
        }
    }

    commitMutation(ClusterMetadataMutation::dropEndpoint(endpoint_name));
    return true;
}

void ClusterMetadataManager::alterEndpoint(const String & endpoint_name, const SettingsChanges & properties)
{
    if (!isEnabled())
        throwIfDisabled();

    if (properties.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER ENDPOINT ... MODIFY PROPERTIES requires at least one assignment");

    EndpointCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!snapshot.endpoints.contains(endpoint_name))
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", endpoint_name);
        record = snapshot.endpoints.at(endpoint_name);
    }

    applyEndpointPropertiesPatch(record, properties);
    commitMutation(ClusterMetadataMutation::alterEndpoint(endpoint_name, record));
}

bool ClusterMetadataManager::createShard(
    const String & shard_name,
    const std::vector<String> & replica_collections,
    UInt32 weight,
    bool internal_replication,
    bool if_not_exists)
{
    if (!isEnabled())
        throwIfDisabled();

    ShardCatalogDefinition definition;
    {
        std::lock_guard lock(mutex);
        if (snapshot.shards.contains(shard_name))
        {
            if (if_not_exists)
                return false;
            throw Exception(ErrorCodes::SHARD_ALREADY_EXISTS, "SQL SHARD `{}` already exists", shard_name);
        }

        assertShardNameAvailable(shard_name);

        definition = buildShardDefinition(shard_name, replica_collections, weight, internal_replication);
    }

    commitMutation(ClusterMetadataMutation::createShard(definition));
    return true;
}

void ClusterMetadataManager::dropShard(const String & shard_name, bool if_exists)
{
    if (!isEnabled())
        throwIfDisabled();

    {
        std::lock_guard lock(mutex);
        if (!snapshot.shards.contains(shard_name))
        {
            if (!if_exists)
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "SQL SHARD `{}` does not exist", shard_name);
            return;
        }

        const auto referencing_clusters = listClustersContainingShard(shard_name);
        if (!referencing_clusters.empty())
        {
            throw Exception(
                ErrorCodes::SHARD_IS_REFERENCED,
                "Cannot drop SQL SHARD `{}` because SQL CLUSTER `{}` references it",
                shard_name,
                referencing_clusters.front());
        }
    }

    commitMutation(ClusterMetadataMutation::dropShard(shard_name));
}

bool ClusterMetadataManager::updateShardPropertiesFromSQL(const ASTAlterShardQuery & query)
{
    if (query.command != AlterShardCommand::ModifyShardProperties)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::updateShardPropertiesFromSQL expects ModifyShardProperties");

    if (!isEnabled())
        throwIfDisabled();

    if (query.shard_definition_properties.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER SHARD ... MODIFY PROPERTIES requires at least one assignment");

    ShardCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!snapshot.shards.contains(query.shard_name))
        {
            if (query.if_exists)
                return false;
            throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", query.shard_name);
        }

        record = snapshot.shards.at(query.shard_name);
        SQLClusterCatalog::PropertyValidation::assertNoDuplicatePropertyNames(query.shard_definition_properties);

        for (const auto & ch : query.shard_definition_properties)
        {
            if (ch.name == "weight")
                record.weight = SQLClusterCatalog::PropertyValidation::Shard::parseWeightValue(ch.value);
            else if (ch.name == "internal_replication")
                SQLClusterCatalog::PropertyValidation::Shard::parseInternalReplicationValue(ch.value, record.internal_replication);
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unknown property `{}` in ALTER SHARD ... MODIFY PROPERTIES (allowed: weight, internal_replication)",
                    ch.name);
            }
        }

        for (const auto & cluster_name : listClustersContainingShard(query.shard_name))
            validateClusterTotalShardWeight(cluster_name, snapshot.clusters.at(cluster_name).members, &record);
    }

    commitAlterShard(record);
    return true;
}

bool ClusterMetadataManager::addReplicaToShardFromSQL(const ASTAlterShardQuery & query)
{
    if (query.command != AlterShardCommand::AddReplica)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::addReplicaToShardFromSQL expects AddReplica");

    if (!isEnabled())
        throwIfDisabled();

    if (query.replica_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ADD REPLICA requires an endpoint name");

    ShardCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!snapshot.shards.contains(query.shard_name))
        {
            if (query.if_exists)
                return false;
            throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", query.shard_name);
        }

        if (!snapshot.endpoints.contains(query.replica_name))
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", query.replica_name);

        record = snapshot.shards.at(query.shard_name);
        if (std::find(record.endpoint_names.begin(), record.endpoint_names.end(), query.replica_name) != record.endpoint_names.end())
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "Endpoint `{}` is already listed on SQL SHARD `{}`",
                query.replica_name,
                query.shard_name);
        }

        const auto & new_endpoint = snapshot.endpoints.at(query.replica_name);
        for (const auto & existing_name : record.endpoint_names)
        {
            const auto it = snapshot.endpoints.find(existing_name);
            if (it != snapshot.endpoints.end() && endpointsMatch(it->second, new_endpoint))
            {
                LOG_WARNING(
                    log,
                    "Endpoint {} points to same address as {} ({}:{})",
                    query.replica_name,
                    existing_name,
                    new_endpoint.host,
                    new_endpoint.port);
                break;
            }
        }

        record.endpoint_names.push_back(query.replica_name);
        resolveEndpointsForShard(record);
    }

    commitAlterShard(record);
    return true;
}

bool ClusterMetadataManager::dropReplicaFromShardFromSQL(const ASTAlterShardQuery & query)
{
    if (query.command != AlterShardCommand::DropReplica)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::dropReplicaFromShardFromSQL expects DropReplica");

    if (!isEnabled())
        throwIfDisabled();

    if (query.replica_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DROP REPLICA requires an endpoint name");

    ShardCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!snapshot.shards.contains(query.shard_name))
        {
            if (query.if_exists)
                return false;
            throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", query.shard_name);
        }

        record = snapshot.shards.at(query.shard_name);
        auto & endpoint_names = record.endpoint_names;
        auto it = std::find(endpoint_names.begin(), endpoint_names.end(), query.replica_name);
        if (it == endpoint_names.end())
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "Endpoint `{}` is not listed on SQL SHARD `{}`",
                query.replica_name,
                query.shard_name);
        }

        if (endpoint_names.size() <= 1)
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cannot DROP the last replica from SQL SHARD `{}`", query.shard_name);

        endpoint_names.erase(it);
        resolveEndpointsForShard(record);
    }

    commitAlterShard(record);
    return true;
}

bool ClusterMetadataManager::replaceShardReplicasFromSQL(const ASTAlterShardQuery & query)
{
    if (query.command != AlterShardCommand::ReplaceReplicas)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::replaceShardReplicasFromSQL expects ReplaceReplicas");

    if (!isEnabled())
        throwIfDisabled();

    if (query.replica_replace_clauses.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE requires at least one FROM/TO list pair");

    ShardCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!snapshot.shards.contains(query.shard_name))
        {
            if (query.if_exists)
                return false;
            throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", query.shard_name);
        }

        record = snapshot.shards.at(query.shard_name);

        std::unordered_map<String, String> repl_map;
        for (const auto & cl : query.replica_replace_clauses)
        {
            if (cl.from_collections.size() != cl.to_collections.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE clause list lengths mismatch");

            for (size_t i = 0; i < cl.from_collections.size(); ++i)
            {
                const String & from_name = cl.from_collections[i];
                const String & to_name = cl.to_collections[i];

                if (!snapshot.endpoints.contains(to_name))
                {
                    throw Exception(
                        ErrorCodes::BAD_CLUSTER_DEFINITION,
                        "Cluster metadata endpoint `{}` does not exist (REPLACE ... TO target must exist)",
                        to_name);
                }

                auto [map_it, inserted] = repl_map.emplace(from_name, to_name);
                if (!inserted && map_it->second != to_name)
                {
                    throw Exception(
                        ErrorCodes::BAD_CLUSTER_DEFINITION,
                        "Conflicting REPLACE mappings for endpoint `{}` on SQL SHARD `{}`",
                        from_name,
                        query.shard_name);
                }
            }
        }

        for (const auto & [from_name, to_name] : repl_map)
        {
            (void)to_name;
            if (std::find(record.endpoint_names.begin(), record.endpoint_names.end(), from_name) == record.endpoint_names.end())
            {
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Endpoint `{}` is not a replica of SQL SHARD `{}`",
                    from_name,
                    query.shard_name);
            }
        }

        for (String & endpoint_name : record.endpoint_names)
        {
            if (auto map_it = repl_map.find(endpoint_name); map_it != repl_map.end())
                endpoint_name = map_it->second;
        }

        std::unordered_set<String> seen;
        for (const auto & endpoint_name : record.endpoint_names)
        {
            if (!seen.insert(endpoint_name).second)
            {
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Duplicate endpoint `{}` after REPLACE on SQL SHARD `{}`",
                    endpoint_name,
                    query.shard_name);
            }
        }

        if (!query.shard_definition_properties.empty())
        {
            SQLClusterCatalog::PropertyValidation::assertNoDuplicatePropertyNames(query.shard_definition_properties);

            for (const auto & ch : query.shard_definition_properties)
            {
                if (ch.name == "weight")
                    record.weight = SQLClusterCatalog::PropertyValidation::Shard::parseWeightValue(ch.value);
                else if (ch.name == "internal_replication")
                    SQLClusterCatalog::PropertyValidation::Shard::parseInternalReplicationValue(ch.value, record.internal_replication);
                else
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Unknown property `{}` in ALTER SHARD ... REPLACE ... MODIFY PROPERTIES (allowed: weight, internal_replication)",
                        ch.name);
                }
            }
        }

        for (const auto & cluster_name : listClustersContainingShard(query.shard_name))
            validateClusterTotalShardWeight(cluster_name, snapshot.clusters.at(cluster_name).members, &record);

        resolveEndpointsForShard(record);
    }

    commitAlterShard(record);
    return true;
}

bool ClusterMetadataManager::createCluster(
    const String & cluster_name,
    const std::vector<String> & members,
    const String & cluster_secret,
    bool allow_distributed_ddl_queries,
    bool if_not_exists)
{
    if (!isEnabled())
        throwIfDisabled();

    if (members.empty())
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "CREATE CLUSTER requires at least one shard member");

    {
        std::lock_guard lock(mutex);
        if (snapshot.clusters.contains(cluster_name))
        {
            if (if_not_exists)
                return false;
            throw Exception(ErrorCodes::CLUSTER_DEFINITION_ALREADY_EXISTS, "SQL CLUSTER `{}` already exists", cluster_name);
        }

        assertClusterNameAvailable(cluster_name);

        for (const auto & member : members)
            validateClusterMemberShardExists(member);
        validateClusterTotalShardWeight(cluster_name, members);
    }

    ClusterCatalogDefinition record;
    record.members = members;
    record.secret = cluster_secret;
    record.allow_distributed_ddl_queries = allow_distributed_ddl_queries;

    commitMutation(ClusterMetadataMutation::createCluster(cluster_name, record));
    return true;
}

bool ClusterMetadataManager::dropCluster(const String & cluster_name, bool if_exists)
{
    if (!isEnabled())
        throwIfDisabled();

    {
        std::lock_guard lock(mutex);
        if (!snapshot.clusters.contains(cluster_name))
        {
            if (if_exists)
                return false;
            throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "SQL CLUSTER `{}` does not exist", cluster_name);
        }
    }

    commitMutation(ClusterMetadataMutation::dropCluster(cluster_name));
    return true;
}

bool ClusterMetadataManager::addClusterMembersFromSQL(const ASTAlterClusterQuery & query)
{
    if (query.command != AlterClusterCommand::AddShard)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::addClusterMembersFromSQL expects AddShard");

    if (!isEnabled())
        throwIfDisabled();

    if (query.add_shard_members.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ADD SHARD requires at least one member name");

    ClusterCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!snapshot.clusters.contains(query.cluster_name))
        {
            if (query.if_exists)
                return false;
            throw Exception(
                ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", query.cluster_name);
        }

        record = snapshot.clusters.at(query.cluster_name);

        for (const auto & member : query.add_shard_members)
        {
            validateClusterMemberShardExists(member);
            if (std::find(record.members.begin(), record.members.end(), member) != record.members.end())
            {
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "SQL CLUSTER member `{}` is already listed in SQL CLUSTER `{}`",
                    member,
                    query.cluster_name);
            }
            record.members.push_back(member);
        }

        validateClusterTotalShardWeight(query.cluster_name, record.members);
    }

    commitAlterCluster(query.cluster_name, record);
    return true;
}

bool ClusterMetadataManager::dropClusterMembersFromSQL(const ASTAlterClusterQuery & query)
{
    if (query.command != AlterClusterCommand::DropShard)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::dropClusterMembersFromSQL expects DropShard");

    if (!isEnabled())
        throwIfDisabled();

    if (query.drop_shard_members.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DROP SHARD requires at least one member name");

    ClusterCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!snapshot.clusters.contains(query.cluster_name))
        {
            if (query.if_exists)
                return false;
            throw Exception(
                ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", query.cluster_name);
        }

        record = snapshot.clusters.at(query.cluster_name);
        auto & mems = record.members;

        for (const auto & member : query.drop_shard_members)
        {
            auto it = std::find(mems.begin(), mems.end(), member);
            if (it == mems.end())
            {
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "SQL CLUSTER member `{}` is not listed in SQL CLUSTER `{}`",
                    member,
                    query.cluster_name);
            }
            mems.erase(it);
        }

        if (mems.empty())
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cannot DROP all members from SQL CLUSTER `{}`", query.cluster_name);
    }

    commitAlterCluster(query.cluster_name, record);
    return true;
}

bool ClusterMetadataManager::replaceClusterMembersFromSQL(const ASTAlterClusterQuery & query)
{
    if (query.command != AlterClusterCommand::ReplaceClusterMembers)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::replaceClusterMembersFromSQL expects ReplaceClusterMembers");

    if (!isEnabled())
        throwIfDisabled();

    if (query.member_replace_clauses.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE requires at least one FROM/TO list pair");

    ClusterCatalogDefinition record;
    {
        std::lock_guard lock(mutex);
        if (!snapshot.clusters.contains(query.cluster_name))
        {
            if (query.if_exists)
                return false;
            throw Exception(
                ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", query.cluster_name);
        }

        record = snapshot.clusters.at(query.cluster_name);

        std::unordered_map<String, String> repl_map;
        for (const auto & cl : query.member_replace_clauses)
        {
            if (cl.from_members.size() != cl.to_members.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE FROM/TO lists must have equal length");

            for (size_t i = 0; i < cl.from_members.size(); ++i)
            {
                const String & from_name = cl.from_members[i];
                const String & to_name = cl.to_members[i];

                validateClusterMemberShardExists(to_name);

                auto [map_it, inserted] = repl_map.emplace(from_name, to_name);
                if (!inserted && map_it->second != to_name)
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

        for (String & member : record.members)
        {
            if (auto map_it = repl_map.find(member); map_it != repl_map.end())
                member = map_it->second;
        }

        std::unordered_set<String> seen;
        for (const auto & member : record.members)
        {
            if (!seen.insert(member).second)
            {
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Duplicate member `{}` after REPLACE on SQL CLUSTER `{}`",
                    member,
                    query.cluster_name);
            }
        }

        validateClusterTotalShardWeight(query.cluster_name, record.members);

        if (!query.cluster_definition_properties.empty())
        {
            SQLClusterCatalog::PropertyValidation::assertNoDuplicatePropertyNames(query.cluster_definition_properties);

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
    }

    commitAlterCluster(query.cluster_name, record);
    return true;
}

}
