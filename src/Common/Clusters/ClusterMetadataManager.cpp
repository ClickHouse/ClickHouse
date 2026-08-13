#include <Common/Clusters/ClusterMetadataManager.h>

#include <Common/Clusters/ClusterFactory.h>
#include <Common/Clusters/PropertyValidation.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/Field.h>
#include <Core/ServerUUID.h>
#include <Interpreters/ClusterMetadataQueryStatusSource.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTAlterClusterQuery.h>
#include <Parsers/ASTAlterShardQuery.h>
#include <Parsers/ASTCreateClusterCatalogQuery.h>
#include <Processors/Sinks/EmptySink.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

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

namespace Setting
{
    extern const SettingsInt64 distributed_ddl_task_timeout;
    extern const SettingsDistributedDDLOutputMode distributed_ddl_output_mode;
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

void applyShardPropertiesPatch(ShardCatalogDefinition & definition, const SettingsChanges & properties)
{
    SQLClusterCatalog::PropertyValidation::assertNoDuplicatePropertyNames(properties);

    for (const auto & change : properties)
    {
        if (change.name == "weight")
            definition.weight = SQLClusterCatalog::PropertyValidation::Shard::parseWeightValue(change.value);
        else if (change.name == "internal_replication")
            SQLClusterCatalog::PropertyValidation::Shard::parseInternalReplicationValue(change.value, definition.internal_replication);
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown property `{}` in ALTER SHARD ... MODIFY PROPERTIES (allowed: weight, internal_replication)",
                change.name);
        }
    }
}

void applyClusterPropertiesPatch(ClusterCatalogDefinition & definition, const SettingsChanges & properties)
{
    SQLClusterCatalog::PropertyValidation::assertNoDuplicatePropertyNames(properties);

    for (const auto & change : properties)
    {
        if (change.name == "secret")
        {
            if (change.value.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `secret` must be a string");
            definition.secret = change.value.safeGet<String>();
        }
        else if (change.name == "allow_distributed_ddl_queries")
        {
            definition.allow_distributed_ddl_queries = parseBoolProperty(change.value);
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown property `{}` in ALTER CLUSTER ... REPLACE ... MODIFY PROPERTIES (allowed: secret, allow_distributed_ddl_queries)",
                change.name);
        }
    }
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
    if (config.root_path.empty())
    {
        LOG_INFO(log, "ClusterMetadataManager initialized without `{}` configuration", CONFIG_PREFIX);
        return;
    }

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

    ddl_worker = std::make_unique<ClusterMetadataDDLWorker>(
        context,
        storage,
        toString(ServerUUID::get()),
        config.keeper_name,
        config.max_log_entries_per_batch,
        [this]
        {
            const String digest = reloadSnapshot();
            requestSnapshotMaterialization();
            return digest;
        },
        [this](const ClusterMetadataMutation & mutation)
        {
            return prepareMutation(mutation);
        },
        [this](const std::vector<ClusterMetadataMutation> & mutations)
        {
            return applyMutations(mutations);
        });
    initialized_replica_group = config.replica_group;

    if (!config.imports.empty())
    {
        importer = std::make_unique<ClusterMetadataImporter>(
            context,
            config.keeper_name,
            config.root_path,
            config.encrypted,
            config.encryption_key_hex,
            config.encryption_algorithm,
            config.imports,
            [this] { requestSnapshotMaterialization(); });
    }

    materialization_task = context->getSchedulePool().createTask(
        StorageID::createEmpty(), "ClusterMetadataMaterializer", [this] { materializationTask(); });

    initialized = true;

    materialization_task->activateAndSchedule();
    requestSnapshotMaterialization();
    ddl_worker->startup();
    if (importer)
        importer->startup();

    LOG_INFO(log, "ClusterMetadataManager initialized for replica group `{}`", initialized_replica_group);
}

void ClusterMetadataManager::shutdown()
{
    /// Flip first so concurrent DDL / materialization callbacks observe disabled state before we
    /// tear down owned components. Stop background work outside `mutex`: their callbacks take it.
    const bool was_initialized = initialized.exchange(false);

    if (importer)
        importer->shutdown();
    if (materialization_task)
        materialization_task->deactivate();
    if (ddl_worker)
        ddl_worker->shutdown();

    std::lock_guard lock(mutex);
    if (was_initialized)
        ClusterFactory::instance().replaceSQLCatalogClusters({});
    importer.reset();
    materialization_task = {};
    ddl_worker.reset();
    materialization_requested = false;
    storage.reset();
    snapshot = {};
    snapshot_version = 0;
    materialized_snapshot_version = 0;
    config = {};
    context = nullptr;
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
    result.keeper_name = config.getString(config_prefix + ".keeper", String(zkutil::DEFAULT_ZOOKEEPER_NAME));
    result.root_path = config.getString(config_prefix + ".path", "");
    result.encrypted = config.getBool(config_prefix + ".encrypted", false);
    if (result.encrypted)
    {
        result.encryption_key_hex = config.getRawString(config_prefix + ".key_hex", "");
        result.encryption_algorithm = config.getString(config_prefix + ".algorithm", "aes_128_ctr");
    }
    result.replica_group = config.getString(config_prefix + ".replica_group", String(DEFAULT_REPLICA_GROUP));
    result.max_log_entries_per_batch = config.getUInt(config_prefix + ".max_log_entries_per_batch", result.max_log_entries_per_batch);

    if (result.root_path.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`{}.path` cannot be empty", config_prefix);
    if (result.max_log_entries_per_batch == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`{}.max_log_entries_per_batch` must be greater than 0", config_prefix);

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
    normalizeSnapshot(snapshot);
    ++snapshot_version;
}

String ClusterMetadataManager::reloadSnapshot()
{
    if (!initialized)
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
    if (!initialized)
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.endpoints.find(endpoint_name);
    if (it == snapshot.endpoints.end())
        return std::nullopt;
    return it->second;
}

std::optional<ShardCatalogDefinition> ClusterMetadataManager::tryGetShard(const String & shard_name) const
{
    if (!initialized)
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.shards.find(shard_name);
    if (it == snapshot.shards.end())
        return std::nullopt;
    return it->second;
}

std::optional<ClusterCatalogDefinition> ClusterMetadataManager::tryGetCluster(const String & cluster_name) const
{
    if (!initialized)
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.clusters.find(cluster_name);
    if (it == snapshot.clusters.end())
        return std::nullopt;
    return it->second;
}

std::vector<String> ClusterMetadataManager::listEndpointNames() const
{
    if (!initialized)
        throwIfDisabled();
    std::lock_guard lock(mutex);
    return listMapKeys(snapshot.endpoints);
}

std::vector<String> ClusterMetadataManager::listShardNames() const
{
    if (!initialized)
        throwIfDisabled();
    std::lock_guard lock(mutex);
    return listMapKeys(snapshot.shards);
}

std::vector<String> ClusterMetadataManager::listClusterNames() const
{
    if (!initialized)
        throwIfDisabled();
    std::lock_guard lock(mutex);
    return listMapKeys(snapshot.clusters);
}

void ClusterMetadataManager::throwIfDisabled() const
{
    throw Exception(
        ErrorCodes::INVALID_CONFIG_PARAMETER,
        "SQL cluster catalog is not initialized; configure `{}` in the server config",
        CONFIG_PREFIX);
}

void ClusterMetadataManager::commitMutation(const ClusterMetadataMutation & mutation)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataManager::commitMutation");
    ddl_worker->enqueueMutationAndConfirmLocal(mutation);
}

BlockIO ClusterMetadataManager::commitMutationSync(const ClusterMetadataMutation & mutation, ContextPtr query_context)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataManager::commitMutationSync");
    auto enqueued = ddl_worker->enqueueMutationForSync(mutation);

    BlockIO io;
    if (enqueued.is_noop)
        return io;

    if (!query_context)
        query_context = context;
    if (!query_context)
        query_context = Context::getGlobalContextInstance();
    if (!query_context)
        return io;

    if (query_context->getSettingsRef()[Setting::distributed_ddl_task_timeout] == 0)
        return io;

    auto source = std::make_shared<ClusterMetadataQueryStatusSource>(
        enqueued.zookeeper_name,
        enqueued.entry_path,
        enqueued.replicas_path,
        query_context,
        enqueued.hosts_to_wait);
    io.pipeline = QueryPipeline(std::move(source));

    if (query_context->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE
        || query_context->getSettingsRef()[Setting::distributed_ddl_output_mode] == DistributedDDLOutputMode::NONE_ONLY_ACTIVE)
        io.pipeline.complete(std::make_shared<EmptySink>(io.pipeline.getSharedHeader()));

    return io;
}

BlockIO ClusterMetadataManager::finishCommit(const ClusterMetadataMutation & mutation, bool sync, ContextPtr query_context)
{
    if (sync)
        return commitMutationSync(mutation, query_context ? query_context : context);
    commitMutation(mutation);
    return {};
}

ClusterMetadataDDLWorker::PreparedMutation ClusterMetadataManager::prepareMutation(const ClusterMetadataMutation & mutation) const
{
    if (!initialized)
        throwIfDisabled();

    ClusterMetadataStoragePtr local_storage;
    ClusterMetadataStorage::Snapshot candidate;
    {
        std::lock_guard lock(mutex);
        local_storage = storage;
        candidate = snapshot;
    }

    if (!local_storage)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Cluster metadata storage is not initialized");

    auto make_noop_prepared = [&]()
    {
        return ClusterMetadataDDLWorker::PreparedMutation{
            .digest = candidate.digest.empty() ? local_storage->calculateDigest(candidate) : candidate.digest,
            .metadata_mutation = mutation,
            .is_noop = true,
        };
    };

    switch (mutation.type)
    {
        case ClusterMetadataMutation::Type::CreateEndpoint:
            if (candidate.endpoints.contains(mutation.name))
            {
                if (mutation.if_not_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` already exists", mutation.name);
            }
            if (candidate.shards.contains(mutation.name))
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL SHARD", mutation.name);
            if (candidate.clusters.contains(mutation.name))
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL CLUSTER", mutation.name);
            break;
        case ClusterMetadataMutation::Type::CreateShard:
            if (candidate.shards.contains(mutation.name))
            {
                if (mutation.if_not_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::SHARD_ALREADY_EXISTS, "SQL SHARD `{}` already exists", mutation.name);
            }
            if (candidate.endpoints.contains(mutation.name))
                throw Exception(
                    ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS,
                    "Cannot create SQL SHARD `{}` because an endpoint with the same name already exists",
                    mutation.name);
            if (candidate.clusters.contains(mutation.name))
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL CLUSTER", mutation.name);
            break;
        case ClusterMetadataMutation::Type::CreateCluster:
            if (candidate.clusters.contains(mutation.name))
            {
                if (mutation.if_not_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_ALREADY_EXISTS, "SQL CLUSTER `{}` already exists", mutation.name);
            }
            if (candidate.endpoints.contains(mutation.name))
                throw Exception(
                    ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS,
                    "Cannot create SQL CLUSTER `{}` because an endpoint with the same name already exists",
                    mutation.name);
            if (candidate.shards.contains(mutation.name))
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_NAME_AMBIGUOUS, "Name `{}` is already used as SQL SHARD", mutation.name);
            for (const auto & shard_name : ClusterCatalogDefinition::deserialize(mutation.definition_data).members)
            {
                if (!candidate.shards.contains(shard_name))
                    throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cluster metadata shard `{}` does not exist", shard_name);
            }
            break;
        case ClusterMetadataMutation::Type::DropEndpoint:
            if (!candidate.endpoints.contains(mutation.name))
            {
                if (mutation.if_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", mutation.name);
            }
            for (const auto & [shard_name, shard] : candidate.shards)
            {
                if (std::find(shard.endpoint_names.begin(), shard.endpoint_names.end(), mutation.name) != shard.endpoint_names.end())
                {
                    throw Exception(
                        ErrorCodes::BAD_CLUSTER_DEFINITION,
                        "Cannot drop cluster metadata endpoint `{}` because shard `{}` references it",
                        mutation.name,
                        shard_name);
                }
            }
            break;
        case ClusterMetadataMutation::Type::DropShard:
        {
            if (!candidate.shards.contains(mutation.name))
            {
                if (mutation.if_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "SQL SHARD `{}` does not exist", mutation.name);
            }
            std::vector<String> referencing_clusters;
            for (const auto & [cluster_name, cluster] : candidate.clusters)
            {
                if (std::find(cluster.members.begin(), cluster.members.end(), mutation.name) != cluster.members.end())
                    referencing_clusters.push_back(cluster_name);
            }
            std::sort(referencing_clusters.begin(), referencing_clusters.end());
            if (!referencing_clusters.empty())
            {
                throw Exception(
                    ErrorCodes::SHARD_IS_REFERENCED,
                    "Cannot drop SQL SHARD `{}` because SQL CLUSTER `{}` references it",
                    mutation.name,
                    referencing_clusters.front());
            }
            break;
        }
        case ClusterMetadataMutation::Type::AlterEndpoint:
        case ClusterMetadataMutation::Type::ModifyEndpointProperties:
            if (!candidate.endpoints.contains(mutation.name))
            {
                if (mutation.if_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", mutation.name);
            }
            break;
        case ClusterMetadataMutation::Type::AlterShard:
            if (!candidate.shards.contains(mutation.name))
            {
                if (mutation.if_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", mutation.name);
            }
            for (const auto & endpoint_name : ShardCatalogDefinition::deserialize(mutation.definition_data).endpoint_names)
            {
                if (!candidate.endpoints.contains(endpoint_name))
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", endpoint_name);
            }
            break;
        case ClusterMetadataMutation::Type::DropCluster:
            if (!candidate.clusters.contains(mutation.name))
            {
                if (mutation.if_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "SQL CLUSTER `{}` does not exist", mutation.name);
            }
            break;
        case ClusterMetadataMutation::Type::AlterCluster:
            if (!candidate.clusters.contains(mutation.name))
            {
                if (mutation.if_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", mutation.name);
            }
            for (const auto & shard_name : ClusterCatalogDefinition::deserialize(mutation.definition_data).members)
            {
                if (!candidate.shards.contains(shard_name))
                    throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cluster metadata shard `{}` does not exist", shard_name);
            }
            break;
        case ClusterMetadataMutation::Type::ModifyShardProperties:
        case ClusterMetadataMutation::Type::AddShardReplicas:
        case ClusterMetadataMutation::Type::DropShardReplicas:
        case ClusterMetadataMutation::Type::ReplaceShardReplicas:
            if (!candidate.shards.contains(mutation.name))
            {
                if (mutation.if_exists)
                    return make_noop_prepared();
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", mutation.name);
            }
            break;
        case ClusterMetadataMutation::Type::AddClusterMembers:
        case ClusterMetadataMutation::Type::DropClusterMembers:
        case ClusterMetadataMutation::Type::ReplaceClusterMembers:
            if (!candidate.clusters.contains(mutation.name))
            {
                if (mutation.if_exists)
                    return make_noop_prepared();
                throw Exception(
                    ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", mutation.name);
            }
            break;
    }

    applyMutationToSnapshot(candidate, mutation);
    validateSnapshotReferences(candidate);
    candidate.digest = local_storage->calculateDigest(candidate);
    return ClusterMetadataDDLWorker::PreparedMutation{
        .digest = candidate.digest,
        .metadata_mutation = materializeMetadataMutation(candidate, mutation),
    };
}

String ClusterMetadataManager::applyMutations(const std::vector<ClusterMetadataMutation> & mutations)
{
    if (!initialized)
        throwIfDisabled();

    std::lock_guard lock(mutex);
    if (!storage)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Cluster metadata storage is not initialized");

    auto candidate = snapshot;
    for (const auto & mutation : mutations)
        applyMutationToSnapshot(candidate, mutation);

    validateSnapshotReferences(candidate);
    candidate.digest = storage->calculateDigest(candidate);
    snapshot = std::move(candidate);
    ++snapshot_version;
    return snapshot.digest;
}

void ClusterMetadataManager::normalizeSnapshot(ClusterMetadataStorage::Snapshot & target) const
{
    for (auto & [shard_name, shard] : target.shards)
    {
        if (shard.name.empty())
            shard.name = shard_name;
        resolveShardEndpoints(shard, target.endpoints);
    }
}

void ClusterMetadataManager::validateSnapshotReferences(const ClusterMetadataStorage::Snapshot & target) const
{
    for (const auto & [shard_name, shard] : target.shards)
    {
        for (const auto & endpoint_name : shard.endpoint_names)
        {
            if (!target.endpoints.contains(endpoint_name))
                throw Exception(
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Cluster metadata shard `{}` references missing endpoint `{}`",
                    shard_name,
                    endpoint_name);
        }
    }

    for (const auto & [cluster_name, cluster] : target.clusters)
    {
        UInt64 total_weight = 0;
        for (const auto & member : cluster.members)
        {
            const auto shard_it = target.shards.find(member);
            if (shard_it == target.shards.end())
                throw Exception(
                    ErrorCodes::SHARD_DOESNT_EXIST,
                    "Cluster metadata cluster `{}` references missing shard `{}`",
                    cluster_name,
                    member);

            total_weight += shard_it->second.weight;
            if (total_weight > Cluster::MAX_TOTAL_SHARD_WEIGHT)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "SQL CLUSTER `{}` total shard weight must not exceed {}, got at least {}",
                    cluster_name,
                    Cluster::MAX_TOTAL_SHARD_WEIGHT,
                    total_weight);
        }
    }
}

ClusterMetadataMutation ClusterMetadataManager::materializeMetadataMutation(
    const ClusterMetadataStorage::Snapshot & source_snapshot,
    const ClusterMetadataMutation & mutation) const
{
    switch (mutation.type)
    {
        case ClusterMetadataMutation::Type::CreateEndpoint:
        case ClusterMetadataMutation::Type::DropEndpoint:
        case ClusterMetadataMutation::Type::AlterEndpoint:
        case ClusterMetadataMutation::Type::CreateShard:
        case ClusterMetadataMutation::Type::DropShard:
        case ClusterMetadataMutation::Type::AlterShard:
        case ClusterMetadataMutation::Type::CreateCluster:
        case ClusterMetadataMutation::Type::DropCluster:
        case ClusterMetadataMutation::Type::AlterCluster:
            return mutation;
        case ClusterMetadataMutation::Type::ModifyEndpointProperties:
        {
            auto prepared = ClusterMetadataMutation::alterEndpoint(mutation.name, source_snapshot.endpoints.at(mutation.name));
            prepared.if_exists = mutation.if_exists;
            return prepared;
        }
        case ClusterMetadataMutation::Type::ModifyShardProperties:
        case ClusterMetadataMutation::Type::AddShardReplicas:
        case ClusterMetadataMutation::Type::DropShardReplicas:
        case ClusterMetadataMutation::Type::ReplaceShardReplicas:
        {
            auto prepared = ClusterMetadataMutation::alterShard(source_snapshot.shards.at(mutation.name));
            prepared.if_exists = mutation.if_exists;
            return prepared;
        }
        case ClusterMetadataMutation::Type::AddClusterMembers:
        case ClusterMetadataMutation::Type::DropClusterMembers:
        case ClusterMetadataMutation::Type::ReplaceClusterMembers:
        {
            auto prepared = ClusterMetadataMutation::alterCluster(mutation.name, source_snapshot.clusters.at(mutation.name));
            prepared.if_exists = mutation.if_exists;
            return prepared;
        }
    }
}

void ClusterMetadataManager::applyMutationToSnapshot(
    ClusterMetadataStorage::Snapshot & target,
    const ClusterMetadataMutation & mutation) const
{
    switch (mutation.type)
    {
        case ClusterMetadataMutation::Type::CreateEndpoint:
        case ClusterMetadataMutation::Type::AlterEndpoint:
        {
            target.endpoints[mutation.name] = EndpointCatalogDefinition::deserialize(mutation.definition_data);
            for (auto & shard_entry : target.shards)
            {
                auto & shard = shard_entry.second;
                if (std::find(shard.endpoint_names.begin(), shard.endpoint_names.end(), mutation.name) != shard.endpoint_names.end())
                    resolveShardEndpoints(shard, target.endpoints);
            }
            break;
        }
        case ClusterMetadataMutation::Type::DropEndpoint:
            target.endpoints.erase(mutation.name);
            break;
        case ClusterMetadataMutation::Type::CreateShard:
        case ClusterMetadataMutation::Type::AlterShard:
        {
            auto shard = ShardCatalogDefinition::deserialize(mutation.definition_data);
            if (shard.name.empty())
                shard.name = mutation.name;
            resolveShardEndpoints(shard, target.endpoints);
            target.shards[mutation.name] = std::move(shard);
            break;
        }
        case ClusterMetadataMutation::Type::DropShard:
            target.shards.erase(mutation.name);
            break;
        case ClusterMetadataMutation::Type::CreateCluster:
        case ClusterMetadataMutation::Type::AlterCluster:
            target.clusters[mutation.name] = ClusterCatalogDefinition::deserialize(mutation.definition_data);
            break;
        case ClusterMetadataMutation::Type::DropCluster:
            target.clusters.erase(mutation.name);
            break;
        case ClusterMetadataMutation::Type::ModifyEndpointProperties:
        {
            auto endpoint_it = target.endpoints.find(mutation.name);
            if (endpoint_it == target.endpoints.end())
            {
                if (mutation.if_exists)
                    break;
                throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", mutation.name);
            }
            applyEndpointPropertiesPatch(endpoint_it->second, mutation.deserializeSettingsChanges());
            /// Keep shard-local endpoint caches in sync with the catalog endpoint map. Materialization reads
            /// `ShardCatalogDefinition::endpoints`, not `endpoint_names` re-resolved from `target.endpoints`.
            for (auto & shard_entry : target.shards)
            {
                auto & shard = shard_entry.second;
                if (std::find(shard.endpoint_names.begin(), shard.endpoint_names.end(), mutation.name) != shard.endpoint_names.end())
                    resolveShardEndpoints(shard, target.endpoints);
            }
            break;
        }
        case ClusterMetadataMutation::Type::ModifyShardProperties:
        {
            auto shard_it = target.shards.find(mutation.name);
            if (shard_it == target.shards.end())
            {
                if (mutation.if_exists)
                    break;
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", mutation.name);
            }
            applyShardPropertiesPatch(shard_it->second, mutation.deserializeSettingsChanges());
            break;
        }
        case ClusterMetadataMutation::Type::AddShardReplicas:
        {
            auto shard_it = target.shards.find(mutation.name);
            if (shard_it == target.shards.end())
            {
                if (mutation.if_exists)
                    break;
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", mutation.name);
            }

            auto & shard = shard_it->second;
            for (const auto & endpoint_name : mutation.deserializeStringList())
            {
                if (!target.endpoints.contains(endpoint_name))
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", endpoint_name);
                if (std::find(shard.endpoint_names.begin(), shard.endpoint_names.end(), endpoint_name) != shard.endpoint_names.end())
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Endpoint `{}` is already listed on SQL SHARD `{}`", endpoint_name, mutation.name);
                shard.endpoint_names.push_back(endpoint_name);
            }
            resolveShardEndpoints(shard, target.endpoints);
            break;
        }
        case ClusterMetadataMutation::Type::DropShardReplicas:
        {
            auto shard_it = target.shards.find(mutation.name);
            if (shard_it == target.shards.end())
            {
                if (mutation.if_exists)
                    break;
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", mutation.name);
            }

            auto & endpoint_names = shard_it->second.endpoint_names;
            for (const auto & endpoint_name : mutation.deserializeStringList())
            {
                auto endpoint_it = std::find(endpoint_names.begin(), endpoint_names.end(), endpoint_name);
                if (endpoint_it == endpoint_names.end())
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Endpoint `{}` is not listed on SQL SHARD `{}`", endpoint_name, mutation.name);
                if (endpoint_names.size() <= 1)
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cannot DROP the last replica from SQL SHARD `{}`", mutation.name);
                endpoint_names.erase(endpoint_it);
            }
            resolveShardEndpoints(shard_it->second, target.endpoints);
            break;
        }
        case ClusterMetadataMutation::Type::ReplaceShardReplicas:
        {
            auto shard_it = target.shards.find(mutation.name);
            if (shard_it == target.shards.end())
            {
                if (mutation.if_exists)
                    break;
                throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cannot alter SQL SHARD `{}`, because it doesn't exist", mutation.name);
            }

            SettingsChanges properties;
            const auto replacements = mutation.deserializeReplacements(&properties);
            std::unordered_map<String, String> replacement_map;
            for (const auto & replacement : replacements)
            {
                if (!target.endpoints.contains(replacement.to))
                    throw Exception(
                        ErrorCodes::BAD_CLUSTER_DEFINITION,
                        "Cluster metadata endpoint `{}` does not exist (REPLACE ... TO target must exist)",
                        replacement.to);
                auto [map_it, inserted] = replacement_map.emplace(replacement.from, replacement.to);
                if (!inserted && map_it->second != replacement.to)
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Conflicting REPLACE mappings for endpoint `{}` on SQL SHARD `{}`", replacement.from, mutation.name);
            }

            auto & shard = shard_it->second;
            for (const auto & [from_name, _] : replacement_map)
            {
                if (std::find(shard.endpoint_names.begin(), shard.endpoint_names.end(), from_name) == shard.endpoint_names.end())
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Endpoint `{}` is not a replica of SQL SHARD `{}`", from_name, mutation.name);
            }

            for (auto & endpoint_name : shard.endpoint_names)
            {
                if (auto map_it = replacement_map.find(endpoint_name); map_it != replacement_map.end())
                    endpoint_name = map_it->second;
            }

            std::unordered_set<String> seen;
            for (const auto & endpoint_name : shard.endpoint_names)
            {
                if (!seen.insert(endpoint_name).second)
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Duplicate endpoint `{}` after REPLACE on SQL SHARD `{}`", endpoint_name, mutation.name);
            }

            applyShardPropertiesPatch(shard, properties);
            resolveShardEndpoints(shard, target.endpoints);
            break;
        }
        case ClusterMetadataMutation::Type::AddClusterMembers:
        {
            auto cluster_it = target.clusters.find(mutation.name);
            if (cluster_it == target.clusters.end())
            {
                if (mutation.if_exists)
                    break;
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", mutation.name);
            }

            auto & members = cluster_it->second.members;
            for (const auto & member : mutation.deserializeStringList())
            {
                if (!target.shards.contains(member))
                    throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cluster metadata shard `{}` does not exist", member);
                if (std::find(members.begin(), members.end(), member) != members.end())
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "SQL CLUSTER member `{}` is already listed in SQL CLUSTER `{}`", member, mutation.name);
                members.push_back(member);
            }
            break;
        }
        case ClusterMetadataMutation::Type::DropClusterMembers:
        {
            auto cluster_it = target.clusters.find(mutation.name);
            if (cluster_it == target.clusters.end())
            {
                if (mutation.if_exists)
                    break;
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", mutation.name);
            }

            auto & members = cluster_it->second.members;
            for (const auto & member : mutation.deserializeStringList())
            {
                auto member_it = std::find(members.begin(), members.end(), member);
                if (member_it == members.end())
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "SQL CLUSTER member `{}` is not listed in SQL CLUSTER `{}`", member, mutation.name);
                members.erase(member_it);
            }
            if (members.empty())
                throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cannot DROP all members from SQL CLUSTER `{}`", mutation.name);
            break;
        }
        case ClusterMetadataMutation::Type::ReplaceClusterMembers:
        {
            auto cluster_it = target.clusters.find(mutation.name);
            if (cluster_it == target.clusters.end())
            {
                if (mutation.if_exists)
                    break;
                throw Exception(ErrorCodes::CLUSTER_DEFINITION_DOESNT_EXIST, "Cannot alter SQL CLUSTER `{}`, because it doesn't exist", mutation.name);
            }

            SettingsChanges properties;
            const auto replacements = mutation.deserializeReplacements(&properties);
            std::unordered_map<String, String> replacement_map;
            for (const auto & replacement : replacements)
            {
                if (!target.shards.contains(replacement.to))
                    throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cluster metadata shard `{}` does not exist", replacement.to);
                auto [map_it, inserted] = replacement_map.emplace(replacement.from, replacement.to);
                if (!inserted && map_it->second != replacement.to)
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Conflicting REPLACE mappings for member `{}` on SQL CLUSTER `{}`", replacement.from, mutation.name);
            }

            auto & cluster = cluster_it->second;
            for (const auto & [from_name, _] : replacement_map)
            {
                if (std::find(cluster.members.begin(), cluster.members.end(), from_name) == cluster.members.end())
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "SQL CLUSTER member `{}` is not listed in SQL CLUSTER `{}`", from_name, mutation.name);
            }

            for (auto & member : cluster.members)
            {
                if (auto map_it = replacement_map.find(member); map_it != replacement_map.end())
                    member = map_it->second;
            }

            std::unordered_set<String> seen;
            for (const auto & member : cluster.members)
            {
                if (!seen.insert(member).second)
                    throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Duplicate member `{}` after REPLACE on SQL CLUSTER `{}`", member, mutation.name);
            }

            applyClusterPropertiesPatch(cluster, properties);
            break;
        }
    }
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

void ClusterMetadataManager::materializeImportedClusters(
    const std::vector<ClusterMetadataImporter::ImportedSnapshot> & imported_snapshots,
    ContextPtr query_context,
    std::map<String, ClusterPtr> & out) const
{
    for (const auto & imported_snapshot : imported_snapshots)
    {
        std::map<String, ClusterPtr> imported_clusters;
        try
        {
            materializeSnapshotClusters(imported_snapshot.snapshot, query_context, imported_clusters);
        }
        catch (...)
        {
            tryLogCurrentException(
                log, fmt::format("Failed to materialize imported clusters from replica group `{}`", imported_snapshot.replica_group));
            continue;
        }

        for (auto & [name, cluster] : imported_clusters)
        {
            if (!out.emplace(name, cluster).second)
                LOG_WARNING(
                    log,
                    "Imported SQL CLUSTER `{}` from replica group `{}` is shadowed by an already-visible cluster of the same name",
                    name,
                    imported_snapshot.replica_group);
        }
    }
}

void ClusterMetadataManager::requestSnapshotMaterialization()
{
    std::lock_guard lock(mutex);
    if (!initialized)
        return;

    materialization_requested = true;
    if (materialization_task)
        materialization_task->schedule();
}

void ClusterMetadataManager::materializationTask()
{
    bool should_publish = false;
    {
        std::lock_guard lock(mutex);
        if (!initialized)
            return;

        should_publish = materialization_requested || snapshot_version != materialized_snapshot_version;
        materialization_requested = false;
    }

    UInt64 published_version = 0;
    if (should_publish)
    {
        try
        {
            published_version = publishSnapshotToClusterFactory();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to materialize cluster metadata into ClusterFactory");
            std::lock_guard lock(mutex);
            if (initialized)
                materialization_requested = true;
        }
    }

    {
        std::lock_guard lock(mutex);
        if (published_version)
            materialized_snapshot_version = published_version;
        if (initialized && materialization_task)
            materialization_task->scheduleAfter(MATERIALIZATION_INTERVAL_MS);
    }
}

UInt64 ClusterMetadataManager::publishSnapshotToClusterFactory() const
{
    ContextPtr local_context;
    ClusterMetadataStorage::Snapshot local_snapshot;
    UInt64 version_snapshot = 0;
    {
        std::lock_guard lock(mutex);
        local_context = context;
        local_snapshot = snapshot;
        version_snapshot = snapshot_version;
    }

    if (!local_context)
        local_context = Context::getGlobalContextInstance();
    if (!local_context)
        return 0;

    std::map<String, ClusterPtr> materialized;
    materializeSnapshotClusters(local_snapshot, local_context, materialized);
    /// Imported (read-only) clusters fill in names not already defined locally.
    /// `importer` lifetime is ordered by shutdown (materialization deactivated before `reset`);
    /// `getLoadedSnapshots` returns empty once importer shutdown has begun.
    if (importer)
        materializeImportedClusters(importer->getLoadedSnapshots(), local_context, materialized);

    for (auto & [_, cluster] : materialized)
        cluster->setDefinitionMetadata(ClusterDefinitionSource::SQLCatalog, version_snapshot);

    ClusterFactory::instance().replaceSQLCatalogClusters(materialized);
    return version_snapshot;
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

String ClusterMetadataManager::getShowCreateShard(const String & shard_name) const
{
    if (!initialized)
        throwIfDisabled();

    std::lock_guard lock(mutex);
    const auto it = snapshot.shards.find(shard_name);
    if (it == snapshot.shards.end())
        throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "SQL SHARD `{}` does not exist", shard_name);

    return formatCreateShardStatement(it->second.name, it->second.endpoint_names, it->second.weight, it->second.internal_replication);
}

String ClusterMetadataManager::getShowCreateCluster(const String & cluster_name) const
{
    if (!initialized)
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

ShardCatalogDefinition ClusterMetadataManager::buildShardDefinition(
    const String & shard_name,
    const std::vector<String> & endpoint_names,
    UInt32 weight,
    bool internal_replication) const
{
    if (endpoint_names.empty())
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "CREATE SHARD requires at least one endpoint");

    ShardCatalogDefinition record;
    record.name = shard_name;
    record.endpoint_names = endpoint_names;
    record.weight = weight;
    record.internal_replication = internal_replication;
    return record;
}

BlockIO ClusterMetadataManager::createEndpoint(
    const String & endpoint_name,
    const EndpointCatalogDefinition & definition,
    bool if_not_exists,
    bool sync, ContextPtr query_context)
{
    if (!initialized)
        throwIfDisabled();

    return finishCommit(
        ClusterMetadataMutation::createEndpoint(endpoint_name, definition, if_not_exists), sync, query_context);
}

BlockIO ClusterMetadataManager::dropEndpoint(const String & endpoint_name, bool if_exists, bool sync, ContextPtr query_context)
{
    if (!initialized)
        throwIfDisabled();

    return finishCommit(ClusterMetadataMutation::dropEndpoint(endpoint_name, if_exists), sync, query_context);
}

BlockIO ClusterMetadataManager::alterEndpoint(const String & endpoint_name, const SettingsChanges & properties, bool sync, ContextPtr query_context)
{
    if (!initialized)
        throwIfDisabled();

    if (properties.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER ENDPOINT ... MODIFY PROPERTIES requires at least one assignment");

    return finishCommit(ClusterMetadataMutation::modifyEndpointProperties(endpoint_name, properties), sync, query_context);
}

BlockIO ClusterMetadataManager::createShard(
    const String & shard_name,
    const std::vector<String> & replica_collections,
    UInt32 weight,
    bool internal_replication,
    bool if_not_exists,
    bool sync, ContextPtr query_context)
{
    if (!initialized)
        throwIfDisabled();

    /// Build the intended definition from the query. Existence / IF NOT EXISTS / name conflicts are
    /// decided in prepareMutation after catch-up (do not copy a possibly-stale local shard record).
    ShardCatalogDefinition definition
        = buildShardDefinition(shard_name, replica_collections, weight, internal_replication);

    return finishCommit(ClusterMetadataMutation::createShard(definition, if_not_exists), sync, query_context);
}

BlockIO ClusterMetadataManager::dropShard(const String & shard_name, bool if_exists, bool sync, ContextPtr query_context)
{
    if (!initialized)
        throwIfDisabled();

    return finishCommit(ClusterMetadataMutation::dropShard(shard_name, if_exists), sync, query_context);
}

BlockIO ClusterMetadataManager::updateShardPropertiesFromSQL(const ASTAlterShardQuery & query, bool sync, ContextPtr query_context)
{
    if (query.command != AlterShardCommand::ModifyShardProperties)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::updateShardPropertiesFromSQL expects ModifyShardProperties");

    if (!initialized)
        throwIfDisabled();

    if (query.shard_definition_properties.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER SHARD ... MODIFY PROPERTIES requires at least one assignment");

    /// Property name/value checks only. Target existence / weight / IF EXISTS are decided in
    /// prepareMutation after catch-up.
    {
        ShardCatalogDefinition record;
        record.name = query.shard_name;
        applyShardPropertiesPatch(record, query.shard_definition_properties);
    }

    return finishCommit(
        ClusterMetadataMutation::modifyShardProperties(query.shard_name, query.shard_definition_properties, query.if_exists),
        sync,
        query_context);
}

BlockIO ClusterMetadataManager::addReplicaToShardFromSQL(const ASTAlterShardQuery & query, bool sync, ContextPtr query_context)
{
    if (query.command != AlterShardCommand::AddReplica)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::addReplicaToShardFromSQL expects AddReplica");

    if (!initialized)
        throwIfDisabled();

    if (query.replica_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ADD REPLICA requires an endpoint name");

    return finishCommit(
        ClusterMetadataMutation::addShardReplicas(query.shard_name, {query.replica_name}, query.if_exists),
        sync,
        query_context);
}

BlockIO ClusterMetadataManager::dropReplicaFromShardFromSQL(const ASTAlterShardQuery & query, bool sync, ContextPtr query_context)
{
    if (query.command != AlterShardCommand::DropReplica)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::dropReplicaFromShardFromSQL expects DropReplica");

    if (!initialized)
        throwIfDisabled();

    if (query.replica_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DROP REPLICA requires an endpoint name");

    return finishCommit(
        ClusterMetadataMutation::dropShardReplicas(query.shard_name, {query.replica_name}, query.if_exists),
        sync,
        query_context);
}

BlockIO ClusterMetadataManager::replaceShardReplicasFromSQL(const ASTAlterShardQuery & query, bool sync, ContextPtr query_context)
{
    if (query.command != AlterShardCommand::ReplaceReplicas)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::replaceShardReplicasFromSQL expects ReplaceReplicas");

    if (!initialized)
        throwIfDisabled();

    if (query.replica_replace_clauses.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE requires at least one FROM/TO list pair");

    std::vector<ClusterMetadataMutation::Replacement> replacements;
    for (const auto & cl : query.replica_replace_clauses)
    {
        if (cl.from_collections.size() != cl.to_collections.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE clause list lengths mismatch");

        for (size_t i = 0; i < cl.from_collections.size(); ++i)
            replacements.push_back({cl.from_collections[i], cl.to_collections[i]});
    }

    /// Property name/value checks only. Replica membership / weight / IF EXISTS are decided in
    /// prepareMutation after catch-up.
    if (!query.shard_definition_properties.empty())
    {
        ShardCatalogDefinition tmp;
        tmp.name = query.shard_name;
        applyShardPropertiesPatch(tmp, query.shard_definition_properties);
    }

    return finishCommit(
        ClusterMetadataMutation::replaceShardReplicas(
            query.shard_name, replacements, query.shard_definition_properties, query.if_exists),
        sync,
        query_context);
}

BlockIO ClusterMetadataManager::createCluster(
    const String & cluster_name,
    const std::vector<String> & members,
    const String & cluster_secret,
    bool allow_distributed_ddl_queries,
    bool if_not_exists,
    bool sync, ContextPtr query_context)
{
    if (!initialized)
        throwIfDisabled();

    if (members.empty())
        throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "CREATE CLUSTER requires at least one shard member");

    ClusterCatalogDefinition record;
    record.members = members;
    record.secret = cluster_secret;
    record.allow_distributed_ddl_queries = allow_distributed_ddl_queries;

    return finishCommit(ClusterMetadataMutation::createCluster(cluster_name, record, if_not_exists), sync, query_context);
}

BlockIO ClusterMetadataManager::dropCluster(const String & cluster_name, bool if_exists, bool sync, ContextPtr query_context)
{
    if (!initialized)
        throwIfDisabled();

    return finishCommit(ClusterMetadataMutation::dropCluster(cluster_name, if_exists), sync, query_context);
}

BlockIO ClusterMetadataManager::addClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync, ContextPtr query_context)
{
    if (query.command != AlterClusterCommand::AddShard)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::addClusterMembersFromSQL expects AddShard");

    if (!initialized)
        throwIfDisabled();

    if (query.add_shard_members.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ADD SHARD requires at least one member name");

    return finishCommit(
        ClusterMetadataMutation::addClusterMembers(query.cluster_name, query.add_shard_members, query.if_exists),
        sync,
        query_context);
}

BlockIO ClusterMetadataManager::dropClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync, ContextPtr query_context)
{
    if (query.command != AlterClusterCommand::DropShard)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::dropClusterMembersFromSQL expects DropShard");

    if (!initialized)
        throwIfDisabled();

    if (query.drop_shard_members.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DROP SHARD requires at least one member name");

    return finishCommit(
        ClusterMetadataMutation::dropClusterMembers(query.cluster_name, query.drop_shard_members, query.if_exists),
        sync,
        query_context);
}

BlockIO ClusterMetadataManager::replaceClusterMembersFromSQL(const ASTAlterClusterQuery & query, bool sync, ContextPtr query_context)
{
    if (query.command != AlterClusterCommand::ReplaceClusterMembers)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataManager::replaceClusterMembersFromSQL expects ReplaceClusterMembers");

    if (!initialized)
        throwIfDisabled();

    if (query.member_replace_clauses.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE requires at least one FROM/TO list pair");

    std::vector<ClusterMetadataMutation::Replacement> replacements;
    for (const auto & cl : query.member_replace_clauses)
    {
        if (cl.from_members.size() != cl.to_members.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "REPLACE FROM/TO lists must have equal length");

        for (size_t i = 0; i < cl.from_members.size(); ++i)
            replacements.push_back({cl.from_members[i], cl.to_members[i]});
    }

    /// Property name/value checks only. Member existence / weight / IF EXISTS are decided in
    /// prepareMutation after catch-up.
    if (!query.cluster_definition_properties.empty())
    {
        ClusterCatalogDefinition tmp;
        applyClusterPropertiesPatch(tmp, query.cluster_definition_properties);
    }

    return finishCommit(
        ClusterMetadataMutation::replaceClusterMembers(
            query.cluster_name, replacements, query.cluster_definition_properties, query.if_exists),
        sync,
        query_context);
}

}
