#include <Common/SQLClusters/SQLClusterFactory.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Core/SettingsFieldsConversionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Parsers/ParserSQLClusterQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <base/sleep.h>

#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace FailPoints
{
    extern const char sql_cluster_create_pause_after_namespace_check[];
}

namespace ErrorCodes
{
    extern const int CLUSTER_ALREADY_EXISTS;
    extern const int CLUSTER_DOESNT_EXIST;
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int LOGICAL_ERROR;
}

namespace
{

using Properties = SettingsChanges;

const std::unordered_set<String> & clusterPropertyKeys()
{
    static const std::unordered_set<String> keys = {"secret", "allow_distributed_ddl_queries"};
    return keys;
}

const std::unordered_set<String> & shardOnlyPropertyKeys()
{
    static const std::unordered_set<String> keys = {"weight", "internal_replication"};
    return keys;
}

const std::unordered_set<String> & replicaPropertyKeys()
{
    static const std::unordered_set<String> keys = {
        "host", "port", "user", "password", "secure", "compression", "priority", "bind_host", "default_database"};
    return keys;
}

bool isCredentialProperty(std::string_view name)
{
    return name == "user" || name == "password";
}

bool isBoolProperty(std::string_view name)
{
    return name == "secure" || name == "compression" || name == "internal_replication"
        || name == "allow_distributed_ddl_queries";
}

void validateAndNormalizeBoolProperty(Field & value, std::string_view name)
{
    if (value.getType() == Field::Types::String)
    {
        const auto & str = value.safeGet<String>();
        if (str.contains("${"))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Property `{}` is a Boolean and must not contain a configuration reference, got `{}`",
                name,
                str);
    }

    try
    {
        value = Field(fieldToNumberSettingValue<bool>(value));
    }
    catch (const Exception &)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Property `{}` must be a Boolean (0, 1, true or false), got {}",
            name,
            value);
    }
}

void validateBoolProperty(const Field & value, std::string_view name)
{
    Field copy = value;
    validateAndNormalizeBoolProperty(copy, name);
}

void normalizeBoolProperties(Properties & properties)
{
    for (auto & change : properties)
    {
        if (isBoolProperty(change.name))
            validateAndNormalizeBoolProperty(change.value, change.name);
    }
}

void validateNoDuplicateProperties(const Properties & properties, std::string_view scope)
{
    std::unordered_set<std::string_view> seen;
    for (const auto & change : properties)
    {
        if (!seen.insert(change.name).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate property `{}` {}", change.name, scope);
    }
}

/// `Cluster::Address` narrows the replica port to `UInt16`. Reject out-of-range
/// values here so a SQL typo cannot be stored and then silently wrap (e.g. `70000` → `4464`).
void validatePortProperty(const Field & value)
{
    Int64 port = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
    if (port < 1 || port > 65535)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Port must be in range 1..65535, got {}", port);
}

/// `Cluster` reads `weight` and `priority` with Poco `getInt`. Negative values used to be
/// stored as `UInt64` (e.g. `weight = -1` → `18446744073709551615`) and then fail with
/// `POCO_EXCEPTION` rather than `BAD_ARGUMENTS`. `weight` is a `UInt32` slot count, so it
/// cannot be negative; `priority` is signed (`lower` means higher priority).
void validateIntProperty(const Field & value, std::string_view name, Int64 min, Int64 max)
{
    Int64 parsed = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
    if (parsed < min || parsed > max)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` must be in range {}..{}, got {}", name, min, max, parsed);
}

void validateClusterLevelProperties(const Properties & properties)
{
    validateNoDuplicateProperties(properties, "at cluster level");

    for (const auto & change : properties)
    {
        if (!clusterPropertyKeys().contains(change.name) && !isCredentialProperty(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` is not allowed at cluster level", change.name);

        if (isBoolProperty(change.name))
            validateBoolProperty(change.value, change.name);
    }
}

void validateShardProperties(const Properties & properties, bool has_replicas)
{
    validateNoDuplicateProperties(properties, "in SHARD");

    for (const auto & change : properties)
    {
        if (has_replicas)
        {
            if (!shardOnlyPropertyKeys().contains(change.name) && !isCredentialProperty(change.name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` is not allowed in SHARD with REPLICA blocks", change.name);
        }
        else if (!replicaPropertyKeys().contains(change.name) && !shardOnlyPropertyKeys().contains(change.name))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` is not allowed in SHARD", change.name);
        }

        if (change.name == "port")
            validatePortProperty(change.value);
        else if (change.name == "weight")
            validateIntProperty(change.value, change.name, 0, std::numeric_limits<Int32>::max());
        else if (change.name == "priority")
            validateIntProperty(change.value, change.name, std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max());
        else if (isBoolProperty(change.name))
            validateBoolProperty(change.value, change.name);
    }
}

void validateReplicaProperties(const Properties & properties)
{
    validateNoDuplicateProperties(properties, "in REPLICA");

    for (const auto & change : properties)
    {
        if (!replicaPropertyKeys().contains(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` is not allowed in REPLICA", change.name);

        if (change.name == "port")
            validatePortProperty(change.value);
        else if (change.name == "priority")
            validateIntProperty(change.value, change.name, std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max());
        else if (isBoolProperty(change.name))
            validateBoolProperty(change.value, change.name);
    }
}

void validateSQLClusterDefinition(const ASTSQLClusterDefinition & definition)
{
    validateClusterLevelProperties(definition.cluster_properties);

    for (const auto & shard_ast : definition.shards)
    {
        const auto & shard = shard_ast->as<const ASTSQLClusterShard &>();
        validateShardProperties(shard.properties, !shard.replicas.empty());

        for (const auto & replica_ast : shard.replicas)
            validateReplicaProperties(replica_ast->as<const ASTSQLClusterReplica &>().properties);
    }
}

void normalizeSQLClusterBooleanProperties(ASTSQLClusterDefinition & definition)
{
    normalizeBoolProperties(definition.cluster_properties);

    for (auto & shard_ast : definition.shards)
    {
        auto & shard = shard_ast->as<ASTSQLClusterShard &>();
        normalizeBoolProperties(shard.properties);
        for (auto & replica_ast : shard.replicas)
            normalizeBoolProperties(replica_ast->as<ASTSQLClusterReplica &>().properties);
    }
}

/// `Clusters::getCluster` expands `{macros}` in the lookup argument. SQL-managed
/// names are stored as literals (metadata path, `create_query`, in-memory key), so a
/// name that contains `{` cannot be created, altered, or dropped: lookup would search
/// for the expansion and miss the stored key, or collide with a different cluster.
void validateSQLClusterName(const String & cluster_name)
{
    if (cluster_name.contains('{'))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SQL cluster name `{}` must not contain macros. Use a literal name; "
            "`cluster()` still expands macros when looking a cluster up",
            cluster_name);
}

/// `MapConfiguration::enumerate` returns keys in lexicographic order, so mixed `shard`/`node`
/// prefixes and unpadded numbers (`1`, `10`, `2`) would not match the SQL definition order.
/// `Cluster` assigns shard and replica numbers from that enumeration.
class OrderedMapConfiguration : public Poco::Util::AbstractConfiguration
{
protected:
    bool getRaw(const std::string & key, std::string & value) const override
    {
        auto it = index.find(key);
        if (it == index.end())
            return false;
        value = entries[it->second].second;
        return true;
    }

    void setRaw(const std::string & key, const std::string & value) override
    {
        auto it = index.find(key);
        if (it != index.end())
        {
            entries[it->second].second = value;
            return;
        }
        index.emplace(key, entries.size());
        entries.emplace_back(key, value);
    }

    void enumerate(const std::string & key, Keys & range) const override
    {
        const std::string prefix = key.empty() ? std::string() : key + ".";
        const size_t prefix_size = prefix.size();
        std::unordered_set<std::string> seen;

        for (const auto & [full_key, _] : entries)
        {
            if (prefix_size != 0 && (full_key.size() <= prefix_size || !full_key.starts_with(prefix)))
                continue;

            const auto end = full_key.find('.', prefix_size);
            std::string sub_key = end == std::string::npos
                ? full_key.substr(prefix_size)
                : full_key.substr(prefix_size, end - prefix_size);

            if (sub_key.empty())
                continue;

            if (seen.insert(sub_key).second)
                range.push_back(std::move(sub_key));
        }
    }

private:
    std::vector<std::pair<std::string, std::string>> entries;
    std::unordered_map<std::string, size_t> index;
};

void setConfigValue(Poco::Util::AbstractConfiguration & config, const String & key, const Field & value)
{
    if (value.getType() == Field::Types::String)
        config.setString(key, value.safeGet<String>());
    else if (value.getType() == Field::Types::Bool)
        config.setBool(key, value.safeGet<bool>());
    else if (value.getType() == Field::Types::Int64)
        config.setInt64(key, value.safeGet<Int64>());
    else if (value.getType() == Field::Types::UInt64)
        config.setUInt64(key, value.safeGet<UInt64>());
    else
        config.setInt64(key, applyVisitor(FieldVisitorConvertToNumber<Int64>(), value));
}

void setSQLClusterConfigValue(
    Poco::Util::AbstractConfiguration & config, const String & key, std::string_view name, const Field & value)
{
    if (isBoolProperty(name))
    {
        Field normalized = value;
        validateAndNormalizeBoolProperty(normalized, name);
        config.setBool(key, normalized.safeGet<bool>());
        return;
    }
    setConfigValue(config, key, value);
}

/// Fill an omitted `port` of one replica (or of an inline `SHARD`) with the listener port of this server.
/// `Cluster::Address` resolves a missing `port` from `tcp_port` or `tcp_port_secure` of the server that
/// materializes the definition. Stored definitions are shared between servers (Keeper metadata storage),
/// so resolving at load time would send identical metadata to different destinations on servers with
/// different listener ports, and fail to load on a server without `tcp_port_secure`.
void resolveOmittedPort(
    Properties & properties,
    const Poco::Util::AbstractConfiguration & server_config,
    const String & cluster_name,
    const String & location)
{
    if (properties.tryGet("port"))
        return;

    bool is_secure = false;
    if (const auto * secure = properties.tryGet("secure"))
    {
        Field normalized = *secure;
        validateAndNormalizeBoolProperty(normalized, "secure");
        is_secure = normalized.safeGet<bool>();
    }

    const char * port_key = is_secure ? "tcp_port_secure" : "tcp_port";
    const Int64 port = server_config.getInt64(port_key, 0);
    if (port < 1 || port > 65535)
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Port is not specified for {} of SQL cluster `{}`, and this server has no `{}` to use as the default",
            location,
            cluster_name,
            port_key);

    properties.emplace_back("port", Field(static_cast<UInt64>(port)));
}

void applyReplicaProperties(Poco::Util::AbstractConfiguration & config, const String & prefix, const Properties & properties)
{
    for (const auto & change : properties)
    {
        if (!replicaPropertyKeys().contains(change.name))
            continue;
        setSQLClusterConfigValue(config, prefix + "." + change.name, change.name, change.value);
    }
}

void applyShardOnlyProperties(Poco::Util::AbstractConfiguration & config, const String & prefix, const Properties & properties)
{
    for (const auto & change : properties)
    {
        if (change.name == "weight" || change.name == "internal_replication")
            setSQLClusterConfigValue(config, prefix + "." + change.name, change.name, change.value);
    }
}

void applyClusterProperties(Poco::Util::AbstractConfiguration & config, const String & prefix, const Properties & properties)
{
    for (const auto & change : properties)
    {
        if (change.name == "secret" || change.name == "allow_distributed_ddl_queries")
            setSQLClusterConfigValue(config, prefix + "." + change.name, change.name, change.value);
        else if (change.name != "user" && change.name != "password")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown cluster property `{}`", change.name);
    }
}

/// Make a definition self-contained before it is persisted: every replica gets an explicit `port`.
void resolveOmittedPorts(ASTSQLClusterDefinition & definition, const Poco::Util::AbstractConfiguration & server_config, const String & cluster_name)
{
    size_t shard_num = 0;
    for (auto & shard_ast : definition.shards)
    {
        ++shard_num;
        auto & shard = shard_ast->as<ASTSQLClusterShard &>();
        if (shard.replicas.empty())
        {
            resolveOmittedPort(shard.properties, server_config, cluster_name, fmt::format("shard {}", shard_num));
            continue;
        }

        size_t replica_num = 0;
        for (auto & replica_ast : shard.replicas)
        {
            ++replica_num;
            resolveOmittedPort(
                replica_ast->as<ASTSQLClusterReplica &>().properties,
                server_config,
                cluster_name,
                fmt::format("shard {} replica {}", shard_num, replica_num));
        }
    }
}

}

ClusterPtr SQLClusterFactory::materializeCluster(
    const ASTCreateSQLClusterQuery & query,
    ContextPtr context,
    String create_statement)
{
    const auto & definition = query.definition->as<const ASTSQLClusterDefinition &>();
    const auto & settings = context->getSettingsRef();

    Poco::AutoPtr<OrderedMapConfiguration> config = new OrderedMapConfiguration();
    const String cluster_prefix = "cluster." + query.cluster_name;

    /// `Cluster::Address` takes the default replica port and detects local replicas by the listener ports of this server.
    const auto & server_config = context->getConfigRef();
    for (const char * port_key : {"tcp_port", "tcp_port_secure"})
    {
        if (server_config.has(port_key))
            config->setString(port_key, server_config.getString(port_key));
    }

    /// clickhouse-local listens on no port of its own, see the comment in `Cluster::Address::Address`.
    const bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;

    applyClusterProperties(*config, cluster_prefix, definition.cluster_properties);

    size_t shard_num = 0;
    for (const auto & shard_ast : definition.shards)
    {
        ++shard_num;
        const auto & shard = shard_ast->as<const ASTSQLClusterShard &>();
        Properties shard_properties = definition.cluster_properties;
        shard_properties.setSettings(shard.properties);

        if (shard.replicas.empty())
        {
            if (!shard_properties.tryGet("host"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard {} in SQL cluster `{}` must have `host` or at least one REPLICA", shard_num, query.cluster_name);

            const String node_prefix = cluster_prefix + ".node" + std::to_string(shard_num);
            applyShardOnlyProperties(*config, node_prefix, shard_properties);
            applyReplicaProperties(*config, node_prefix, shard_properties);
        }
        else
        {
            const String shard_prefix = cluster_prefix + ".shard" + std::to_string(shard_num);
            applyShardOnlyProperties(*config, shard_prefix + ".", shard_properties);

            size_t replica_num = 0;
            for (const auto & replica_ast : shard.replicas)
            {
                ++replica_num;
                const auto & replica = replica_ast->as<const ASTSQLClusterReplica &>();
                Properties replica_properties = shard_properties;
                replica_properties.setSettings(replica.properties);
                applyReplicaProperties(*config, shard_prefix + ".replica" + std::to_string(replica_num), replica_properties);
            }
        }
    }

    if (shard_num == 0)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "SQL cluster `{}` must contain at least one shard", query.cluster_name);

    return std::make_shared<Cluster>(
        *config,
        settings,
        "cluster",
        query.cluster_name,
        treat_local_port_as_remote,
        Cluster::SourceId::SQL,
        std::move(create_statement));
}

ClusterPtr SQLClusterFactory::rematerialize(const Cluster & cluster, ContextPtr context)
{
    if (cluster.getSourceId() != Cluster::SourceId::SQL)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot rematerialize a non-SQL cluster");

    const auto & create_query = cluster.getSQLCreateQuery();
    if (create_query.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "SQL cluster `{}` has no stored CREATE statement", cluster.getName());

    const auto & settings = context->getSettingsRef();
    ParserCreateSQLClusterQuery parser;
    const auto ast = parseQuery(
        parser,
        create_query,
        "SQL cluster " + cluster.getName(),
        /*max_query_size=*/0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    return materializeCluster(ast->as<const ASTCreateSQLClusterQuery &>(), context, create_query);
}

SQLClusterFactory & SQLClusterFactory::instance()
{
    static SQLClusterFactory factory;
    return factory;
}

void SQLClusterFactory::shutdown()
{
    shutdown_called = true;
    if (update_task)
        update_task->deactivate();
    std::lock_guard lock(mutex);
    metadata_storage.reset();
}

void SQLClusterFactory::loadIfNot()
{
    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);
}

bool SQLClusterFactory::usesReplicatedStorage()
{
    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);
    return metadata_storage->isReplicated();
}

void SQLClusterFactory::loadIfNotImpl(std::lock_guard<std::mutex> &)
{
    if (loaded)
        return;

    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    metadata_storage = SQLClusterMetadataStorage::create(context);

    reloadFromStorage();

    if (metadata_storage->isReplicated())
    {
        update_task = context->getSchedulePool()->createTask(StorageID::createEmpty(), "SQLClusterMetadataStorage", [this] { updateFunc(); });
        update_task->activate();
        update_task->schedule();
    }

    loaded = true;
}

void SQLClusterFactory::reloadFromStorage()
{
    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    const auto clusters = metadata_storage->getAll();

    std::unordered_set<String> snapshot_names;
    snapshot_names.reserve(clusters.size());
    for (const auto & [cluster_name, _] : clusters)
        snapshot_names.insert(cluster_name);

    /// Names absent from a consistent snapshot are already dropped. Remove them before materializing
    /// the rest: a surviving definition can throw and must not keep serving an unrelated deleted cluster.
    for (auto it = stored_cluster_names.begin(); it != stored_cluster_names.end();)
    {
        if (snapshot_names.contains(*it))
        {
            ++it;
            continue;
        }

        context->removeSQLCluster(*it);
        it = stored_cluster_names.erase(it);
    }

    std::vector<std::pair<String, ClusterPtr>> materialized;
    materialized.reserve(clusters.size());
    std::unordered_set<String> new_stored_cluster_names;
    new_stored_cluster_names.reserve(clusters.size());

    for (const auto & [cluster_name, create_query] : clusters)
    {
        /// Apply the same namespace check as `CREATE CLUSTER`: a stored SQL cluster must not shadow a cluster
        /// from the server configuration, cluster discovery or a `Replicated` database that appeared later.
        if (auto existing = context->tryGetCluster(cluster_name); existing && existing->getSourceId() != Cluster::SourceId::SQL)
        {
            LOG_ERROR(log, "Not loading SQL cluster `{}`: a cluster with the same name already exists (source: {})",
                cluster_name, Cluster::formatSourceIdForSystemTable(existing->getSourceId()));
            continue;
        }
        if (isReplicatedDatabaseClusterName(cluster_name))
        {
            LOG_ERROR(log, "Not loading SQL cluster `{}`: a `Replicated` database owns the name", cluster_name);
            continue;
        }

        auto create_statement = create_query.formatWithSecretsOneLine();
        materialized.emplace_back(cluster_name, materializeCluster(create_query, context, std::move(create_statement)));
        new_stored_cluster_names.insert(cluster_name);
    }

    for (const auto & cluster_name : stored_cluster_names)
    {
        if (new_stored_cluster_names.contains(cluster_name))
            continue;

        /// Still in the snapshot but skipped: another source owns the name.
        /// `DISCOVERY` / `REPLICATED_DATABASE` clusters live outside the static map, so a previously
        /// loaded SQL cluster with the same name is still there and must be removed. `CONFIG` clusters
        /// share that map and must survive (`removeSQLCluster` also refuses them).
        auto existing = context->tryGetCluster(cluster_name);
        if ((!existing && !isReplicatedDatabaseClusterName(cluster_name)) || (existing && existing->getSourceId() == Cluster::SourceId::CONFIG))
            continue;

        context->removeSQLCluster(cluster_name);
    }

    for (auto & [cluster_name, cluster] : materialized)
    {
        /// A `Replicated` database could have been created after the check above. Recheck under the database guard
        /// (see `createFromSQL`), held until the cluster is installed, so a concurrent `CREATE DATABASE` either sees
        /// the SQL cluster or is seen here.
        auto database_guard = getDDLGuardForReplicatedDatabaseClusterName(cluster_name);
        if (isReplicatedDatabaseClusterName(cluster_name))
        {
            LOG_ERROR(log, "Not loading SQL cluster `{}`: a `Replicated` database owns the name", cluster_name);
            new_stored_cluster_names.erase(cluster_name);
            context->removeSQLCluster(cluster_name);
            continue;
        }

        if (context->trySetSQLCluster(cluster_name, cluster))
            continue;

        LOG_ERROR(log, "Not loading SQL cluster `{}`: a cluster with the same name already exists", cluster_name);
        new_stored_cluster_names.erase(cluster_name);
        /// A leftover SQL cluster in the static map must not keep shadowing a discovered name.
        context->removeSQLCluster(cluster_name);
    }

    stored_cluster_names = std::move(new_stored_cluster_names);

    /// Advance the loaded version only now that the new snapshot has been fully read and applied,
    /// so a reload that throws above does not skip the failed version on the next `waitUpdate`.
    metadata_storage->commitReload();
}

void SQLClusterFactory::createFromSQL(const ASTCreateSQLClusterQuery & query)
{
    validateSQLClusterName(query.cluster_name);

    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);

    if (metadata_storage->exists(query.cluster_name))
    {
        if (query.if_not_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "SQL cluster `{}` already exists", query.cluster_name);
    }

    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    if (auto existing = context->tryGetCluster(query.cluster_name))
    {
        if (existing->getSourceId() == Cluster::SourceId::SQL)
            throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "SQL cluster `{}` already exists", query.cluster_name);

        if (existing->getSourceId() == Cluster::SourceId::CONFIG)
            throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Cluster `{}` already exists in server configuration", query.cluster_name);

        throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Cluster `{}` already exists (cluster discovery)", query.cluster_name);
    }

    /// A `Replicated` database owns its name even if it cannot build its cluster at the moment, so `tryGetCluster`
    /// above returns nothing. `RENAME DATABASE` and `CREATE DATABASE` take this guard, so the check is not racy.
    /// The guard is taken for the database that would own the name, i.e. without the all-groups prefix, and is held
    /// until the cluster is installed.
    auto database_guard = getDDLGuardForReplicatedDatabaseClusterName(query.cluster_name);
    if (isReplicatedDatabaseClusterName(query.cluster_name))
        throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Cluster `{}` already exists (`Replicated` database)", query.cluster_name);

    FailPointInjection::pauseFailPoint(FailPoints::sql_cluster_create_pause_after_namespace_check);

    validateSQLClusterDefinition(query.definition->as<const ASTSQLClusterDefinition &>());

    auto resolved_ast = query.clone();
    auto & resolved_query = resolved_ast->as<ASTCreateSQLClusterQuery &>();
    auto & resolved_definition = resolved_query.definition->as<ASTSQLClusterDefinition &>();
    normalizeSQLClusterBooleanProperties(resolved_definition);
    resolveOmittedPorts(resolved_definition, context->getConfigRef(), query.cluster_name);

    /// Build the in-memory cluster first, then persist, as named collections and handlers do.
    /// A definition that `materializeCluster` rejects (missing `host`, invalid `weight`, ...)
    /// must not leave a file or Keeper znode that then fails `CREATE CLUSTER` with
    /// `CLUSTER_ALREADY_EXISTS` and blocks other servers at `loadIfNot`.
    auto create_statement = resolved_query.formatWithSecretsOneLine();
    auto cluster = materializeCluster(resolved_query, context, create_statement);
    context->installSQLCluster(
        query.cluster_name,
        cluster,
        [&]
        {
            metadata_storage->writeCreateQuery(query.cluster_name, create_statement, false);
        },
        /*allow_replace_sql=*/ false);
    stored_cluster_names.insert(query.cluster_name);
}

void SQLClusterFactory::alterFromSQL(const ASTAlterSQLClusterQuery & query)
{
    validateSQLClusterName(query.cluster_name);

    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);

    if (!metadata_storage->exists(query.cluster_name))
    {
        /// Another replica may have dropped the cluster before this one has synced; do not keep serving the stale copy.
        removeStaleLocalCluster(query.cluster_name);
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "SQL cluster `{}` does not exist", query.cluster_name);
    }

    /// Apply the same namespace check as `CREATE CLUSTER` and the reload: the stored SQL cluster may have been
    /// created on another replica, where it does not collide with a cluster of another source existing on this one.
    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    if (auto existing = context->tryGetCluster(query.cluster_name); existing && existing->getSourceId() != Cluster::SourceId::SQL)
        throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Cannot alter SQL cluster `{}`: a cluster with the same name already exists (source: {})",
            query.cluster_name, Cluster::formatSourceIdForSystemTable(existing->getSourceId()));
    /// Held until the cluster is installed, as in `createFromSQL`.
    auto database_guard = getDDLGuardForReplicatedDatabaseClusterName(query.cluster_name);
    if (isReplicatedDatabaseClusterName(query.cluster_name))
        throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Cannot alter SQL cluster `{}`: a `Replicated` database owns the name",
            query.cluster_name);

    ASTCreateSQLClusterQuery create_query;
    create_query.cluster_name = query.cluster_name;
    create_query.definition = query.definition->clone();

    validateSQLClusterDefinition(create_query.definition->as<const ASTSQLClusterDefinition &>());
    auto & alter_definition = create_query.definition->as<ASTSQLClusterDefinition &>();
    normalizeSQLClusterBooleanProperties(alter_definition);
    resolveOmittedPorts(alter_definition, context->getConfigRef(), query.cluster_name);

    auto create_statement = create_query.formatWithSecretsOneLine();
    auto cluster = materializeCluster(create_query, context, create_statement);
    context->installSQLCluster(
        query.cluster_name,
        cluster,
        [&]
        {
            metadata_storage->writeCreateQuery(query.cluster_name, create_statement, true);
        },
        /*allow_replace_sql=*/ true);
    /// The cluster may have been created on another replica and not yet loaded here by the background reload.
    /// Register it, so that a later reload or `DROP CLUSTER` removes it once it is gone from the metadata storage.
    stored_cluster_names.insert(query.cluster_name);
}

void SQLClusterFactory::dropFromSQL(const ASTDropSQLClusterQuery & query)
{
    validateSQLClusterName(query.cluster_name);

    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);

    const bool removed = metadata_storage->removeIfExists(query.cluster_name);
    if (!removed)
    {
        /// Another replica may have dropped the cluster before this one has synced; do not keep serving the stale copy.
        removeStaleLocalCluster(query.cluster_name);
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "SQL cluster `{}` does not exist", query.cluster_name);
    }

    /// The metadata is removed regardless of the local state: with shared metadata storage, the SQL cluster may be
    /// skipped on this server because of a cluster with the same name from another source, which must be kept.
    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    context->removeSQLCluster(query.cluster_name);
    stored_cluster_names.erase(query.cluster_name);

    LOG_INFO(log, "Dropped SQL cluster `{}`", query.cluster_name);
}

void SQLClusterFactory::removeStaleLocalCluster(const String & cluster_name)
{
    if (!stored_cluster_names.erase(cluster_name))
        return;

    if (!Context::getGlobalContextInstance()->getGlobalContext()->removeSQLCluster(cluster_name))
        return;
    LOG_INFO(log, "Removed SQL cluster `{}` which no longer exists in the metadata storage", cluster_name);
}

void SQLClusterFactory::updateFunc()
{
    LOG_TRACE(log, "SQL cluster metadata background updating thread started");

    while (!shutdown_called.load())
    {
        try
        {
            if (metadata_storage->waitUpdate())
            {
                std::lock_guard lock(mutex);
                if (metadata_storage)
                    reloadFromStorage();
            }
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                LOG_INFO(log, "Lost ZooKeeper connection while syncing SQL clusters, will retry: {}", getCurrentExceptionMessage(true));
                sleepForSeconds(1);
            }
            else
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
        }
        catch (...)
        {
            /// A shared definition that `CREATE CLUSTER` or `ALTER CLUSTER` would not produce, e.g. one edited
            /// by hand in Keeper, may not be materializable here. Keep the current snapshot: the loaded
            /// version is not advanced, so the reload is retried on the next update.
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    LOG_TRACE(log, "SQL cluster metadata background updating thread finished");
}

}
